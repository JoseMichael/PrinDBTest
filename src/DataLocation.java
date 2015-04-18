public class DataLocation {
	private String TableName;
	private int BucketNo;
	private int RowNo;
	private String DataString;
	private String DataType;

	public DataLocation() {
		//
	}

	public DataLocation(String tname, int bno, int rno, String ds) {
		this.TableName = tname;
		this.BucketNo = bno;
		this.RowNo = rno;
		this.DataString = ds;
	}

	public String getTableName() {
		return TableName;
	}

	public void setTableName(String tname) {
		TableName = tname;
	}

	public int getBucketNo() {
		return BucketNo;
	}

	public void setBucketNo(int bid) {
		BucketNo = bid;
	}

	public int getRowNo() {
		return RowNo;
	}

	public void setRowNo(int rid) {
		RowNo = rid;
	}

	public String getDataString() {
		return DataString;
	}

	public void setDataString(String data) {
		DataString = data;
	}

	public String getDataType() {
		return DataType;
	}

	public void setDataType(String datatype) {
		DataType = datatype;
	}
}