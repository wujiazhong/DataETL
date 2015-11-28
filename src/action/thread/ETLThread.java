package action.thread;

import java.util.ArrayList;

import indi.feature.action.DataObj;
import interfaces.getDataObjListAction;

public class ETLThread extends Thread{
	private getDataObjListAction action = null;
	private int startDiaID = 0;
	private int endDiaID = 0;
	private ArrayList<ArrayList<DataObj>> uniDataList = null;
	private boolean getDataSuccess = false;
	
	public ETLThread(getDataObjListAction Action, final int StartDiaID, final int EndDiaID){
		super();	
		this.action = Action;
		this.startDiaID = StartDiaID;
		this.endDiaID = EndDiaID;
	}
	
	public void run(){ 
		try{
			setUniDataList(action.getDataObjList(startDiaID, endDiaID));
			if(getUniDataList()!=null && !getUniDataList().isEmpty()){
				this.setGetDataSuccess(true);
				Thread.sleep(1000*5);
			}
		} catch (Exception e) {
			try {
				throw new InterruptedException(this.getName()+" stopped.");
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
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
}
