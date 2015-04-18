//this is the class for meta data for main memory row store pages
public class MetaMMR 
{
	
	int actualBucketNumber=-999;
	boolean isNewEntry[] = new boolean[16];
	String TableName;
	
	public void initMetaPage(int BcktNo, String tablename)
	{
		setTableName(tablename);
		setBcktNo(BcktNo);
	}
	
	public void setTableName(String tablename)
	{
		TableName = tablename;
	}
	
	public void setBcktNo(int BcktNo)
	{
		actualBucketNumber = BcktNo;
	}
	
	public void setNewEntries(int pos)
	{
		int i;
		for(i=0; i<pos; i++)
			isNewEntry[i] = false;
		while(i!=16)
		{
			isNewEntry[i] = true;
			i++;
		}
	}
	

}
