import java.util.List;
import java.util.ArrayList;

public class PageColumnMetaName
{
	private int PageId;							// Page ID
	private String TimeStamp;					// Time Stamp for LRU Replacement
	private List<DataLocation> Data = new ArrayList<DataLocation>();
	
	public PageColumnMetaName()
	{
		//Default Constructor
	}
	
	//Copy Constructor
	public PageColumnMetaName(PageColumnMetaName another)
	{
		this.PageId = another.getPageId();
		this.TimeStamp = another.getTimeStamp();
		this.Data = new ArrayList<DataLocation>(another.getData());
	}
	
	//Getters and Setters
	public int getPageId()
	{
		return PageId;
	}
	public void setPageId(int id)
	{
		PageId = id;
	}
	public String getTimeStamp()
	{
		return TimeStamp;
	}
	public void setTimeStamp(String time)
	{
		TimeStamp = time;
	}
	public List<DataLocation> getData()
	{
		return Data;
	}
	public void setData(List<DataLocation> data)
	{
		Data = new ArrayList<DataLocation>(data);
	}
	public int calcDataSize()
	{
		int datasize = 0;
		for(DataLocation rdata: this.Data)
		{
			datasize += rdata.getDataString().length();
		}
		return datasize;
	}
}