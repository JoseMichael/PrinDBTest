
public class MMRowStorePages {

	String ID[] = new String[16];
	String Name[] = new String[16];
	String PhoneNo[] = new String[16];
	String TableName[] = new String[16];  //this is meta data
	int TransactionNumber[] = new int[16]; //this is also meta data
	
	
	int UsedForInserts = 0;
	
	public MMRowStorePages()
	{
		UsedForInserts = 0;
		
		for(int i=0; i<16; i++)
		{
			TransactionNumber[i]=-1;
		}//initializes all transaction numbers to -1
	}
	
	public boolean isEmpty()
	{
		//TODO make changes to this to actually check if empty , think I fixed it
		if(ID[0]==null)
		{
			return true;
		}
		else
			return false;
	}
	
	public void displayRows()
	{
		System.out.println("Contents of this Row");
		for(int i=0; i<16; i++)
		{
			if(ID[i]==null)
				break;
			System.out.println(ID[i]+"\t"+Name[i]+"\t"+PhoneNo[i]);
		}
		System.out.println("End of contents");
	}
	
	public int nextFreeRow()
	{
		int i;
		for(i=0; i<16; i++)
		{
			if(ID[i]==null)
				break;
		}
		return i;
	}
	
}
