package indi.feature.action;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import common.util.DB;
import action.thread.ETLThread;
import indi.feature.action.DataObj;
import interfaces.getDataObjListAction;

public class CalcDistAction implements getDataObjListAction{
	private Connection conn = null;
	private final static int threadNum = 32;                //task num
	private final static int p_Num = 100;                   //divide the whole stream into p_num area
	private final static int entry_Num = 100;
	private final static int diff_Load_Satrt_Index = 1;
	private final static int dn_Start_Index = 101;
	private ArrayList<String> srcDiagList = null;
	private ArrayList<String> tarDiagList = null;
	private final String srcTable = "DB2INST1.T_INDICATOR";
	private final String tarTable = "DB2INST1.T_INDI_FEATURE";
	@SuppressWarnings("unused")
	private int zeroLoadNum = 0;
	
	public CalcDistAction() {
		conn = DB.getConnection();
		setSrcDiagList(getDiagList(srcTable));
		setTarDiagList(getDiagList(tarTable));
	}
	
	/**
	 * Get diagram list in table
	 * @param tableName
	 * @return diagram list
	 */
	private ArrayList<String> getDiagList(final String tableName) {
		print("start to get diagram list from "+tableName);
		Statement stm = DB.getStatement(conn);
		ArrayList<String> diagList = new ArrayList<String>();

		ResultSet res = null;
		try{
			//String getDiaList_sql = "select distinct DIAGRAM_ID from DB2INST1.T_INDICATOR order by 1 asc fetch first 100 row only";
			String getDiaList_sql = "select distinct DIAGRAM_ID from " + tableName + " order by 1 asc";
			res = DB.getResultSet(stm,getDiaList_sql);
			
			while(res.next()){
				diagList.add(res.getString("DIAGRAM_ID"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			DB.close(res);
			DB.close(stm);
		}
		print("Get dia list from "+tableName+". It contains "+diagList.size()+" items.");		
		return diagList;
	}
	
	/**
	 * @author wujz
	 * @param starting diagram id, end diagram id
	 * @exception NumberFormatException, SQLException
	 * @return data list containing all load and shift of diagram between the starting diagram id and the end diagram id
	 */
	public ArrayList<ArrayList<DataObj>> getDataObjList(final int startDiaID, final int endDiaID) throws NumberFormatException, SQLException {
		ArrayList<ArrayList<DataObj>> uniDataList = new ArrayList<ArrayList<DataObj>>();
		
		String getDistData_sql = "select rank() over(partition by DIAGRAM_ID order by INDICATOR_ID asc) as ROW_ID, DIAGRAM_ID as DIAGRAM_ID, INDICATOR_ID as INDICATOR_ID, SHIFT as SHIFT, LOAD as LOAD  "
				               + "from DB2INST1.T_INDICATOR where DIAGRAM_ID between ? and ?";
		PreparedStatement pstmt = DB.prepare(conn, getDistData_sql);
		ResultSet res = null;
		try{
			pstmt.setInt(1, startDiaID);
			pstmt.setInt(2, endDiaID);
			res = pstmt.executeQuery();
			print("Start to get diagram from "+startDiaID+" to "+endDiaID);

			int lastDiagID = -1;
			ArrayList<DataObj> dataList = null;
			
			boolean isValidDataSet = true;
			boolean isCurLoadPositive = true;
			
			while(res.next()){
				int diagID = res.getInt("DIAGRAM_ID");
				double loadValue = res.getDouble("LOAD");
				double shift = res.getDouble("SHIFT");
				isCurLoadPositive = loadValue>=0?true:false;
					
				// if it is a new diagram id
				if(lastDiagID != diagID){
					lastDiagID = diagID;

					if(dataList!=null && !dataList.isEmpty()){
						uniDataList.add(dataList);
					} 

					dataList = new ArrayList<DataObj>();
					isValidDataSet = isCurLoadPositive;
				}
				
				// if it is NOT a new diagram id, judge if last load value is negative
				if(!isValidDataSet)
					continue;
				
				isValidDataSet = isCurLoadPositive;
				if(isValidDataSet){
					dataList.add(new DataObj(String.valueOf(diagID),res.getInt("INDICATOR_ID"), shift, loadValue));
				} else {
					print("Failure in Diagram ("+diagID+"): Load at shift("+shift+") is negative.");
					dataList.clear();
				}	
			}
			// add the data list
			if(dataList!=null && !dataList.isEmpty()){
				uniDataList.add(dataList);
			}
			
			print("Complete getting diagram from "+startDiaID+" to "+(endDiaID - 1));
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DB.close(pstmt);
			DB.close(res);
		}
		
		return uniDataList;
	}	

	public void closeCalcDisAction(){
		print("close connection to db2");
		DB.close(conn);
	}
	
	private String createColStr(){
		String insert_sql = "insert into DB2INST1.T_INDI_FEATURE (DIAGRAM_ID,";
		for(int i=diff_Load_Satrt_Index; i<dn_Start_Index+entry_Num;i++){
			insert_sql += "F_"+String.valueOf(i)+",";
		}
		insert_sql = insert_sql.substring(0,insert_sql.length()-1)+") values ";
		return insert_sql;
	}
	
	public void initFeatureTable() throws Exception{	
		print("Start init T_INDI_FEATURE");
			
		int diagListSize = getSrcDiagList().size();
		int diagNumPerThread = diagListSize/threadNum + 1;
		
		print("Start to create multi threads ...");
		ETLThread[] t = new ETLThread[threadNum];
		
		try{
			ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
			for (int i = 0; i < threadNum; i++){   
				int startIndex = i * diagNumPerThread;
				int startDiagID = Integer.valueOf(getSrcDiagList().get(startIndex));
				
				int endIndex = (startIndex + diagNumPerThread - 1) >= diagListSize ? (diagListSize-1) : (startIndex + diagNumPerThread - 1);
				int endDiagID = Integer.valueOf(getSrcDiagList().get(endIndex));
				
				t[i] = new ETLThread(this, startDiagID, endDiagID, i);
				threadPool.execute(t[i]);
				print("Thread "+i+" start ...");	
	        } 
			threadPool.shutdown();
			try { 
	            boolean loop = true; 
	            int waitTime = 2;
	            do {
	                loop = !threadPool.awaitTermination(waitTime, TimeUnit.SECONDS); 
	            } while(loop); 
	        } catch (InterruptedException e) { 
	            e.printStackTrace(); 
	        } 

			Thread.sleep(1000);
			
/*			for(int i = 0; i < threadNum; i++){
				ArrayList<ArrayList<DataObj>> uniDataList = t[i].getUniDataList();
				insertData(uniDataList, i);
			}*/
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void insertData(ArrayList<ArrayList<DataObj>> uniDataList, final int threadIndex){
		String insert_sql = createColStr();
		final int MAX_ROW = 1000; 
		int BATCH_NUM = 1;
		final int uniDataList_size = uniDataList.size();

		Statement stmt = DB.getStatement(conn);
		print("start to insert data in task "+threadIndex);
		try{
			conn.setAutoCommit(true);
			int validDataListNumInCurBatch = 0;
			for(int i=0; i<uniDataList_size; i++){
				ArrayList<DataObj> dataList = uniDataList.get(i);
				boolean isValidDataSet = true; 
				//divide the distance into p_Num area, start from 1st point to calculate the diff load
				double div = calcMaxDistance(dataList)/p_Num;
				double curDist = 0.0;
				
				double[] halfLoadList = getHalfLoadList(dataList);
				if (isLoadZero(halfLoadList)){
					zeroLoadNum++;
					isValidDataSet = false;
				}

				int sortedtype = getShiftVariedType(dataList);
				if(isValidDataSet){
					if (sortedtype == 0) {
						print("Failure in Diagram ("+dataList.get(0).getDiag_id()+"): Oscillation in shift order.");
						isValidDataSet = false;
					} else {					
						if (sortedtype == -1) {
							normalizeDataList(dataList);
						} 
	
						if (isShiftReversed(halfLoadList)){
							swap(dataList);
						}
					}
					
					List<String> record = new ArrayList<String>();
					List<String> dnstreamList = new ArrayList<String>();
					record.add(dataList.get(0).getDiag_id());
					for(int j=0; j<p_Num; j++){					
						//leave out the point whose dist=0
						curDist = div*(j+1);
						List<Integer> dataPosList = new ArrayList<Integer>(getNearestPoint(dataList,curDist));
						
						if(dataPosList == null || dataPosList.isEmpty()){
							isValidDataSet = false;
							print("Failure in Diagram ("+dataList.get(0).getDiag_id()+"): Fail to interpolate the point at "+curDist+". Div is "+div);
							break;
						}
						
						double upLoad = (dataList.get(dataPosList.get(0)).getLoad() + dataList.get(dataPosList.get(1)).getLoad())/2;  
						double dnLoad = (dataList.get(dataPosList.get(2)).getLoad() + dataList.get(dataPosList.get(3)).getLoad())/2;  
						dnstreamList.add(String.valueOf(dnLoad));
						double diffLoad = upLoad - dnLoad;
						record.add(String.valueOf(diffLoad));
					}
					
					if(isValidDataSet){
						String val_sql = "(";
						val_sql += record.get(0)+",";
						int record_size = record.size();
						for(int k=2; k<=record_size; k++){
							val_sql += record.get(k-1)+",";
						}
						for(int t=record_size+1; t<=record_size+dnstreamList.size(); t++){
							val_sql += dnstreamList.get(t-record_size-1)+",";
						}
						insert_sql += val_sql.substring(0, val_sql.length()-1)+"),";
						validDataListNumInCurBatch++;
					}
				}

				if((i == MAX_ROW*BATCH_NUM || i == uniDataList_size-1) && validDataListNumInCurBatch != 0){
					insert_sql = insert_sql.substring(0, insert_sql.length()-1);
					stmt.addBatch(insert_sql);
					insert_sql = createColStr();
					print("insert "+BATCH_NUM+"th batch in task "+threadIndex);
					BATCH_NUM++;
					validDataListNumInCurBatch = 0;
				}
			}
			int[] counts = stmt.executeBatch();
			if(counts!=null){
				print("totally insert "+counts.length+" batch");
			}
			print("complete to insert data in task "+threadIndex);
			conn.commit();
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			DB.close(stmt);
		}			
	}
	
	/**
	 * 
	 * @author wujz
	 * @param dataList
	 * @return Shift varied type: 1 is normal; -1 is reversed; 0 is abnormal
	 * @throws Exception
	 */
	private int getShiftVariedType(ArrayList<DataObj> dataList) throws Exception{
		if(dataList==null || dataList.isEmpty()){
			throw new Exception("dataList is null or empty!");
		}
		
		int flag = 0;
		int listSize = dataList.size();
		int maxPos = getExtremeShiftPos(dataList, "max");
		double maxLastShift = dataList.get(0).getShift();
		boolean isFirstHalfAsc = false;
		for(int i=0; i<=maxPos; i++){
			if(maxLastShift > dataList.get(i).getShift()){
				break;
			} 
			
			maxLastShift = dataList.get(i).getShift();
			if(i==maxPos){
				isFirstHalfAsc = true;
			}
		}
		
		boolean isSecHalfDesc = false;
		maxLastShift = dataList.get(maxPos).getShift();
		for(int i=maxPos; i<listSize; i++){
			if(maxLastShift < dataList.get(i).getShift()){
				break;
			} 
			
			maxLastShift = dataList.get(i).getShift();
			if(i==listSize-1){
				isSecHalfDesc = true;
			}
		}
		
		if(isFirstHalfAsc&&isSecHalfDesc){
			flag = 1;
		}
		
		int minPos = getExtremeShiftPos(dataList, "min");
		double minLastShift = dataList.get(0).getShift();
		boolean isFirstHalfDesc = false;
		for(int i=0; i<=minPos; i++){
			if(minLastShift < dataList.get(i).getShift()){
				break;
			} 
			
			minLastShift = dataList.get(i).getShift();
			if(i==minPos){
				isFirstHalfDesc = true;
			}
		}
		
		boolean isSecHalfAsc = false;
		minLastShift = dataList.get(minPos).getShift();
		for(int i=minPos; i<listSize; i++){
			if(minLastShift > dataList.get(i).getShift()){
				break;
			} 
			
			minLastShift = dataList.get(i).getShift();
			if(i==listSize-1){
				isSecHalfAsc = true;
			}
		}
		
		if(isFirstHalfDesc && isSecHalfAsc){
			flag = -1;
		}

		return flag;
	}
	
	/**
	 * Get max or min point of the stream
	 * @param dataList
	 * @param flag
	 * @return
	 * @throws Exception
	 */
	private int getExtremeShiftPos(ArrayList<DataObj> dataList, final String flag) throws Exception{
		if(dataList==null || dataList.isEmpty()){
			throw new Exception("dataList is null or empty!");
		}
		
		int pos = 0;
		double value = dataList.get(0).getShift();
		for(int i=0; i<dataList.size(); i++){
			if(flag.equals("min")){
				if(value > dataList.get(i).getShift()){
					value = dataList.get(i).getShift();
					pos = i;
				}
			} else if(flag.equals("max")) {
				if(value < dataList.get(i).getShift()){
					value = dataList.get(i).getShift();
					pos = i;
				}
			}
		}
		
		return pos;
	}
	
	/**
	 * For shift of data that is not aligned normally (first increase then decrease), use this method to realign the shift in data order
	 * @author wujz
	 * @param dataList
	 * @throws Exception
	 */
	private void normalizeDataList(ArrayList<DataObj> dataList) throws Exception{
		if(dataList==null || dataList.isEmpty()){
			throw new Exception("dataList is null or empty!");
		}
		
		int dataSize = dataList.size();
		int halfSize = dataSize/2;
		int quaterSize = halfSize/2;
		DataObj tempObj = null;
		
		for(int i=0; i<quaterSize; i++){
			tempObj = dataList.get(halfSize-i-1).clone();
			dataList.set(halfSize-i-1, dataList.get(i).clone());
			dataList.set(i, tempObj);
		}	
		
		for(int j=halfSize;j<=halfSize+quaterSize;j++){			
			int indx = j-halfSize;
			tempObj = dataList.get(dataSize-indx-1).clone();
			dataList.set(dataSize-indx-1, dataList.get(j).clone());
			dataList.set(j, tempObj);
		}
	}
	
	/**
	 * Judge if the load of first half stream is less second one.
	 * @param dataList
	 * @return
	 * @throws Exception
	 */
	private boolean isShiftReversed(double[] halfLoadList) throws Exception{
		boolean isStreamReversed = false;
		if(halfLoadList==null || halfLoadList.length != 2){
			throw new Exception("dataList is null or empty!");
		}
		if(halfLoadList[0] < halfLoadList[1])
			isStreamReversed = true;
		return isStreamReversed;
	}
	
	/**
	 * Check if load is zero
	 * @author wujz
	 * @param halfLoadList
	 * @return 
	 * @throws Exception
	 */
	private boolean isLoadZero(double[] halfLoadList) throws Exception{
		boolean isZeroLoad = false;
		if(halfLoadList == null || halfLoadList.length != 2){
			throw new Exception("dataList is null or empty!");
		}
		if(halfLoadList[0] == 0.0 || halfLoadList[1] == 0.0)
			isZeroLoad = true;
		return isZeroLoad;
	}
	
	private double[] getHalfLoadList(ArrayList<DataObj> dataList) throws Exception{
		double[] halfLoadList = {0,0};
		if(dataList == null || dataList.isEmpty()){
			throw new Exception("dataList is null or empty!");
		}
		
		int dataSize = dataList.size();
		int halfSize = dataSize/2;
		double firstHalfMeanSum = 0.0;
		for(int i=0; i<halfSize; i++)
			firstHalfMeanSum += dataList.get(i).getLoad();
		halfLoadList[0] = firstHalfMeanSum;
		
		double secHalfMeanSum = 0.0;
		for(int i=halfSize;i<dataSize;i++)
			secHalfMeanSum += dataList.get(i).getLoad();
		halfLoadList[1] = secHalfMeanSum;
		return halfLoadList;
	}
	
	/**
	 * Swap load if the stream is upside down.
	 * @author wujz
	 * @param dataList
	 * @return
	 * @throws Exception
	 */
	private void swap(ArrayList<DataObj> dataList) throws Exception{
		if(dataList==null || dataList.isEmpty()){
			throw new Exception("dataList is null or empty!");
		}
		
		int dataSize = dataList.size();
		int halfSize = dataSize/2;
		DataObj tempObj = null;
		for(int i=0;i<halfSize;i++){
			tempObj = dataList.get(dataSize-i-1).clone();
			dataList.set(dataSize-i-1, dataList.get(i).clone());
			dataList.set(i, tempObj);			
		}
	}
	
	/**
	 * Calculate max dist in the stream
	 * @param dataList
	 * @return
	 * @throws Exception
	 */
	private double calcMaxDistance(ArrayList<DataObj> dataList) throws Exception{
		if(dataList==null || dataList.isEmpty()){
			throw new Exception("dataList is null or empty!");
		}
		
		double max = dataList.get(0).getShift();
		double min = dataList.get(0).getShift();
		for(int i=0; i<dataList.size(); i++){
			if(max < dataList.get(i).getShift())
				max = dataList.get(i).getShift();
			if(min > dataList.get(i).getShift())
				min = dataList.get(i).getShift();
		}
		return (max-min);
	}
	
	/**
	 * Insert points into stream
	 * @param dataList
	 * @param dist
	 * @return shift list
	 * @throws Exception
	 */
	private List<Integer> getNearestPoint(ArrayList<DataObj> dataList, final double dist) throws Exception{
		if(dataList==null || dataList.isEmpty()){
			throw new Exception("dataList is null or empty!");
		}
		List<Integer> dataPosList = new ArrayList<Integer>();
		final int listSize = dataList.size();

		//upstream
		int[] upDesPos = {-1,-1};
		for(int i=0; i<listSize/2; i++){
			if(i<listSize/2-1 && dist >= dataList.get(i).getShift() && dist <= dataList.get(i+1).getShift()){
				upDesPos[0] = i;
				upDesPos[1] = i+1;
				break;
			} else if(i==listSize/2-1) {
				upDesPos[0] = i;
				upDesPos[1] = i+1;
				break;
			}
		}
		//downstream
		int[] dnDesPos = {-1,-1};
		for(int i=listSize/2; i<listSize-1; i++){
			if(i<listSize-1 && dist >= dataList.get(i+1).getShift() && dist <= dataList.get(i).getShift()){
				dnDesPos[0] = i+1;
				dnDesPos[1] = i;
				break;
			} else if(i==listSize-2) {
				dnDesPos[0] = i+1;
				dnDesPos[1] = i;
				break;
			}
		}
		dataPosList.add(upDesPos[0]);
		dataPosList.add(upDesPos[1]);
		dataPosList.add(dnDesPos[0]);
		dataPosList.add(dnDesPos[1]);
		
		boolean isValidPosList = true;
		for(int i=0;i<dataPosList.size();i++){
			if(dataPosList.get(i) == -1){
				isValidPosList = false;
				break;
			}
		}
		
		if(!isValidPosList){
			dataPosList.clear();
		}
		
		return dataPosList;
	}
	
	/**
	 * clear target table
	 * @author wujz
	 */
	private void clearFeatureTable(){
		String del_sql = "delete from DB2INST1.T_INDI_FEATURE where DIAGRAM_ID between ? and ?";
		
		print("Start to delete data from T_INDI_FEATURE.");
		final int DEL_NUM = 5000;
		int diagListSize = getTarDiagList().size();
		
		if(diagListSize == 0){
			print("No record in DB2INST1.T_INDI_FEATURE.");
			return;
		}
		
		PreparedStatement pstmt = null;
		try{
			for(int i=0;i<diagListSize/DEL_NUM+1;i++){
				int startIndex = i * DEL_NUM;
				int endIndex = (startIndex + DEL_NUM) >= diagListSize ? (diagListSize - 1) : (startIndex + DEL_NUM - 1);
				
				pstmt = DB.prepare(conn, del_sql);
				pstmt.setInt(1, Integer.valueOf(getTarDiagList().get(startIndex)));
				pstmt.setInt(2, Integer.valueOf(getTarDiagList().get(endIndex)));
				pstmt.executeUpdate();
				DB.close(pstmt);
				print("Prepare to delete diagram id between " + getTarDiagList().get(startIndex) + " and " + getTarDiagList().get(endIndex));
			}
			print("Complete deleting data");
		} catch (SQLException e) {
			e.printStackTrace();
			print(e.getNextException());
		} catch (Exception e) {
			e.printStackTrace();
			print(e.getMessage());
		} finally {
			DB.close(pstmt);
		}
	}
	
	public static void print(Object msg){
		System.out.println(getSysTime()+":  "+String.valueOf(msg));
	}
	
	public static String getSysTime(){
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return String.valueOf(df.format(new Date()));
	}
	
	public ArrayList<String> getSrcDiagList() {
		return srcDiagList;
	}

	private void setSrcDiagList(ArrayList<String> srcDiagList) {
		this.srcDiagList = srcDiagList;
	}
	
	public ArrayList<String> getTarDiagList() {
		return tarDiagList;
	}

	private void setTarDiagList(ArrayList<String> tarDiagList) {
		this.tarDiagList = tarDiagList;
	}
	
	public static void main(String[] args){
		CalcDistAction dataObj =  new CalcDistAction();
		try{
			dataObj.clearFeatureTable();
			dataObj.initFeatureTable();
		} catch (Exception e) {
			e.printStackTrace();
		}
		dataObj.closeCalcDisAction();
	}

}
