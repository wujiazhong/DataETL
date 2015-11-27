package calcDis_action;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import db.DB;

public class CalcDisAction {
	private Connection conn = null;
	private final static int p_Num = 100;
	private final static int entry_Num = 100;
	private final static int diff_Load_Satrt_Index = 1;
	private final static int dn_Start_Index = 101;
	
	public CalcDisAction(){
		conn = DB.getConnection();
	}
	
	public void closeCalcDisAction(){
		print("close connection to db2");
		DB.close(conn);
	}
	
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
			if(i>0 && dist >= dataList.get(i+1).getShift() && dist <= dataList.get(i).getShift()){
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
		return dataPosList;
	}
	
/*	private static String createParaStr(){
		String para_str = "({0},";
		for(int i=diff_Load_Satrt_Index; i<dn_Start_Index+entry_Num;i++){
			para_str += "{"+i+"},";
		}
		para_str = para_str.substring(0,para_str.length()-1)+")";
		return para_str;
	}*/
	
	public static String createColStr(){
		String insert_sql = "insert into DB2INST1.T_INDI_FEATURE (DIAGRAM_ID,";
		for(int i=diff_Load_Satrt_Index; i<dn_Start_Index+entry_Num;i++){
			insert_sql += "F_"+String.valueOf(i)+",";
		}
		insert_sql = insert_sql.substring(0,insert_sql.length()-1)+") values ";
		return insert_sql;
	}
	
	public void insertPoints() throws Exception{		
		ArrayList<ArrayList<DataObj>> uniDataList = getDataList();
		if(uniDataList==null || uniDataList.isEmpty()){
			throw new Exception("uniDataList is null or empty!");
		}
		
		String insert_sql = createColStr();
		final int MAX_ROW = 500; 
		int BATCH_NUM = 1;
		final int uniDataList_size = uniDataList.size();
		//TO DO
		Statement stmt = DB.getStatement(conn);
		//PreparedStatement pstmt = DB.prepare(conn, insert_sql);
		print("start to insert data: ");
		printSysTime();
		try{
			conn.setAutoCommit(true);
			for(int i=0; i<uniDataList_size; i++){
				ArrayList<DataObj> dataList = uniDataList.get(i);
				print("i="+i);
				print("start to get data for diagram: "+dataList.get(0).getDiag_id());
				//divide the distance into p_Num area, start from 1st point to calculate the diff load
				double div = calcMaxDistance(dataList)/p_Num;
				double curDist = 0.0;
				
				int sortedtype = getSortedType(dataList);
				print("diagram id="+dataList.get(0).getDiag_id());
				print("sortedtype="+sortedtype);
				if (sortedtype==-1) {
					normalizDataList(dataList);
				} else if (sortedtype==0) {
					continue;
				}
				
				List<String> record = new ArrayList<String>();
				List<String> dnstreamList = new ArrayList<String>();
				record.add(dataList.get(0).getDiag_id());
				print("diagram_id="+record.get(0));
				for(int j=0; j<p_Num; j++){					
					//leave out the point whose dist=0
					curDist = div*(j+1);
					List<Integer> dataPosList = new ArrayList<Integer>(getNearestPoint(dataList,curDist));
					
					double upLoad = (dataList.get(dataPosList.get(0)).getLoad() + dataList.get(dataPosList.get(1)).getLoad())/2;  
					double dnLoad = (dataList.get(dataPosList.get(2)).getLoad() + dataList.get(dataPosList.get(3)).getLoad())/2;  
					dnstreamList.add(String.valueOf(dnLoad));
					double diffLoad = upLoad - dnLoad;
					record.add(String.valueOf(diffLoad));
				}
								
				String val_sql = "(";
				val_sql += record.get(0)+",";
				//pstmt.setString(1,record.get(0));
				int record_size = record.size();
				for(int k=2; k<=record_size; k++){
					//pstmt.setDouble(k, Double.valueOf(record.get(k-1)));
					val_sql += record.get(k-1)+",";
				}
				for(int t=record_size+1; t<=record_size+dnstreamList.size(); t++){
					//pstmt.setDouble(t, Double.valueOf(dnstreamList.get(t-record_size-1)));
					val_sql += dnstreamList.get(t-record_size-1)+",";
				}
				insert_sql += val_sql.substring(0, val_sql.length()-1)+"),";
				
				if(i==MAX_ROW*BATCH_NUM || i==uniDataList_size-1){
					insert_sql = insert_sql.substring(0, insert_sql.length()-1);
					stmt.addBatch(insert_sql);
					insert_sql = createColStr();
					print("insert "+BATCH_NUM+"th batch");
					BATCH_NUM++;
				}
				//pstmt.addBatch();
			}
			//stmt.executeUpdate(insert_sql);
			int[] counts = stmt.executeBatch();
			if(counts!=null){
				print("totally insert "+counts.length+" batch");
			}
			print("complete to insert data: ");
			printSysTime();
			conn.commit();
			CalcDisAction.print("success!");
		} catch (Exception e){
			e.printStackTrace();
			CalcDisAction.print("fail!");
		} finally {
			DB.close(stmt);
/*			try{
				pstmt.close();
				pstmt = null;
			} catch (SQLException e) {
				e.printStackTrace();
			}*/
		}			
	}
	
	/**
	 * @author wujz
	 * @return a data list containing all data from each diagram in T_INDICATOR
	 * @throws NumberFormatException
	 * @throws SQLException
	 */
	private ArrayList<ArrayList<DataObj>> getDataList() throws NumberFormatException, SQLException{
		ArrayList<String> diaList = getDiaIDList();
		ResultSet res = null;
		PreparedStatement pstmt = null;
		ArrayList<ArrayList<DataObj>> uniDataList = new ArrayList<ArrayList<DataObj>>();
		
		print("start to get data for each diagram ...");
		print("start to get data list: ");
		printSysTime();
		String getDistData_sql = "select DIAGRAM_ID as DIAGRAM_ID, INDICATOR_ID as INDICATOR_ID, SHIFT as SHIFT, LOAD as LOAD  from DB2INST1.T_INDICATOR where DIAGRAM_ID=?  order by INDICATOR_ID asc";
		
		for(int i=0; i<diaList.size(); i++){	
			print("i="+i);
			print("diagram_id is: "+diaList.get(i));
			ArrayList<DataObj> dataList = new ArrayList<DataObj>();
			pstmt = DB.prepare(conn,  getDistData_sql);
			pstmt.setInt(1,Integer.valueOf(diaList.get(i)));
			
			boolean isLoadPositive = true;
			
			try{
				res = pstmt.executeQuery();
				while(res.next()){
					double loadValue = res.getDouble("LOAD");
					
					if(loadValue<0){
						isLoadPositive = false;
						break;
					}
					
					dataList.add(new DataObj(String.valueOf(res.getInt("DIAGRAM_ID")),res.getInt("INDICATOR_ID"), res.getDouble("SHIFT"), loadValue));
				}
				if(isLoadPositive) uniDataList.add(dataList);
				
			}  catch (Exception e) {
				e.printStackTrace();
			} finally {
				try{
					pstmt.close();
					pstmt = null;
					DB.close(res);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		print("complete getting data for each diagram ...");
		printSysTime();
		return uniDataList;
	}
	
	/**
	 * @author wujz
	 * @return a list contains all diagram id
	 */
	private ArrayList<String> getDiaIDList(){
		print("start to get diagram list: ");
		printSysTime();
		Statement stm = DB.getStatement(conn);
		ArrayList<String> diaList = new ArrayList<String>();
		ResultSet res = null;
		print("start to get diagram list");
		try{
			//String getDiaList_sql = "select distinct DIAGRAM_ID from DB2INST1.T_INDICATOR  order by 1 asc fetch first 3 row only";
			String getDiaList_sql = "select distinct DIAGRAM_ID, COLLECT_DATETIME from DB2INST1.T_INDICATOR order by 2 desc fetch first 5000 row only";
			res = DB.getResultSet(stm,getDiaList_sql);
			
			while(res.next()){
				diaList.add(res.getString("DIAGRAM_ID"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			DB.close(res);
			DB.close(stm);
		}
		print("Get dia list from db! It contains "+diaList.size()+" items.");
		print("complete to get diagram list: ");
		printSysTime();
		return diaList;
	}
	
	private int getSortedType(ArrayList<DataObj> dataList) throws Exception{
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
	
	private void normalizDataList(ArrayList<DataObj> dataList) throws Exception{
		if(dataList==null || dataList.isEmpty()){
			throw new Exception("dataList is null or empty!");
		}
		
		int dataSize = dataList.size();
		int halfSize = dataSize/2;
		int quaterSize = halfSize/2;
		DataObj tempObj = null;
		double firstHalfMeanLoad = 0.0;
		for(int i=0; i<quaterSize; i++){
			//dataList.get(i).setIndi_id(halfSize-i-1);
			//dataList.get(halfSize-i-1).setIndi_id(i);
			
			tempObj = dataList.get(halfSize-i-1).clone();
			dataList.set(halfSize-i-1, dataList.get(i).clone());
			dataList.set(i, tempObj);
			firstHalfMeanLoad += dataList.get(i).getLoad()+dataList.get(halfSize-i-1).getLoad();
		}	
		
		double secHalfMeanLoad = 0.0;
		for(int j=halfSize;j<=halfSize+quaterSize;j++){			
			int indx = j-halfSize;
			//dataList.get(j).setIndi_id(dataSize-indx-1);
			//dataList.get(dataSize-indx-1).setIndi_id(j);
			
			tempObj = dataList.get(dataSize-indx-1).clone();
			dataList.set(dataSize-indx-1, dataList.get(j).clone());
			dataList.set(j, tempObj);
			secHalfMeanLoad += dataList.get(j).getLoad()+dataList.get(dataSize-indx-1).getLoad();
		}
		
		if(firstHalfMeanLoad<secHalfMeanLoad){
			swap(dataList);
		}

	}
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
	public static void print(Object msg){
		System.out.println(String.valueOf(msg));
	}
	
	public static void printSysTime(){
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//è®¾ç½®æ—¥æœŸæ ¼å¼�
		print(df.format(new Date()));// new Date()ä¸ºèŽ·å�–å½“å‰�ç³»ç»Ÿæ—¶é—´
	}
	
	public static void main(String[] args) throws Exception{
		CalcDisAction dataObj =  new CalcDisAction();
		CalcDisAction.print("start insert data to T_INDI_FEATURE");
		dataObj.insertPoints();
		dataObj.closeCalcDisAction();
	}
}
