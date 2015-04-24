import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


/**
 * @author Priyanka
 *
 */
public class TransProc {
	
	public List<String> script=new ArrayList<>();
	public List<String> completedOperations=new ArrayList<>();
	public int index=-1;
	public int scriptNum;
	public boolean isWaiting=false;
	public boolean isProcess=false;
	public List<LockItems> lockItem=new ArrayList<LockItems>();
	public boolean isOver=false;
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public List<LockItems> getLockItem() {
		return lockItem;
	}

	public void setLockItem(List<LockItems> lockItem) {
		this.lockItem = lockItem;
	}

	public String getNextOperation() 
	{
		String operation="";
		int index=getIndex();
		if(index++<=script.size())
		{
			setIndex(index++);
			if(index<script.size())
				operation=script.get(getIndex());
			else
				operation="";
		}
		return operation;
	}
	
	public void decrementScriptPointer()
	{
		int index=getIndex();
		setIndex(index--);
	}

	public void skipToTransactionEnd() {
		int indexVal=index;
		while(true)
		{
			String op=script.get(indexVal);
			StringTokenizer token=new StringTokenizer(op);
			if(token.hasMoreElements())
			{
				if(token.equals("C") || token.equals("A"))
				{
					index=indexVal;
					break;
				}
			}
			indexVal++;
		}
		
	}
	
	public void isCompleted() 
	{
		if(index==script.size())
			isOver=true;
		
	}
}
