package action.thread;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import indi.feature.action.DataObj;
import interfaces.getDataObjListAction;

public class ETLThread extends Thread{
	private getDataObjListAction action = null;
	private int startDiaID = 0;
	private int endDiaID = 0;
	private ArrayList<ArrayList<DataObj>> uniDataList = null;
	private boolean getDataSuccess = false;
	private int taskIndex = -1;
	
	public ETLThread(getDataObjListAction Action, final int StartDiaID, final int EndDiaID, final int TaskIndex){
		super();	
		this.action = Action;
		this.startDiaID = StartDiaID;
		this.endDiaID = EndDiaID;
		this.setTaskIndex(TaskIndex);
	}
	
	public void run(){ 
		try{
			setUniDataList(action.getDataObjList(startDiaID, endDiaID));
			action.insertData(getUniDataList(), getTaskIndex());
			if(getUniDataList()!=null && !getUniDataList().isEmpty()){
				this.setGetDataSuccess(true);
				Thread.sleep(1000*1);
				
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				System.out.println(String.valueOf(df.format(new Date()))+": task "+getTaskIndex()+" is done!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ArrayList<ArrayList<DataObj>> getUniDataList() {
		return uniDataList;
	}

	private void setUniDataList(ArrayList<ArrayList<DataObj>> arrayList) {
		this.uniDataList = arrayList;
	}

	public boolean isGetDataSuccess() {
		return getDataSuccess;
	}

	public void setGetDataSuccess(boolean getDataSuccess) {
		this.getDataSuccess = getDataSuccess;
	}

	public int getTaskIndex() {
		return this.taskIndex;
	}

	public void setTaskIndex(final int TaskNum) {
		this.taskIndex = TaskNum;
	}  
}
