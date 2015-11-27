package calcDis_action;

public class DataObj {
	private String diag_id = null;
	private int indi_id = -1;
	private double shift = 0.0;
	private double load = 0.0;
	
	public DataObj(final String DIAG_ID, final int INDI_ID, final double SHIFT, final double LOAD){
		setDiag_id(DIAG_ID);
		setIndi_id(INDI_ID);
		setShift(SHIFT);
		setLoad(LOAD);
	}
	
	public DataObj clone(){
	    return new DataObj(this.getDiag_id(),this.getIndi_id(), this.getShift(), this.getLoad());
	}
	
	public DataObj(DataObj obj) throws Exception{
		if(obj == null){
			throw new Exception("null pointer when use DataObj.assignValue");
		}
		setDiag_id(obj.getDiag_id());
		setIndi_id(obj.getIndi_id());
		setShift(obj.getShift());
		setLoad(obj.getLoad());
	}

	public int getIndi_id() {
		return indi_id;
	}

	public void setIndi_id(int indi_id) {
		this.indi_id = indi_id;
	}

	public double getShift() {
		return shift;
	}

	public void setShift(double shift) {
		this.shift = shift;
	}

	public double getLoad() {
		return load;
	}

	public void setLoad(double load) {
		this.load = load;
	}

	public String getDiag_id() {
		return diag_id;
	}

	public void setDiag_id(String diag_id) {
		this.diag_id = diag_id;
	}
}
