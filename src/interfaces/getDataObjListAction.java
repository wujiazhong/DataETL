package interfaces;

import java.sql.SQLException;
import java.util.ArrayList;
import indi.feature.action.DataObj;

public interface getDataObjListAction {
	public ArrayList<ArrayList<DataObj>> getDataObjList(final int startDiaID, final int endDiaID) throws NumberFormatException, SQLException;
	public void insertData(ArrayList<ArrayList<DataObj>> uniDataList, final int threadIndex);
}
