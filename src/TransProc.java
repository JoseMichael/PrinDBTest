import java.util.ArrayList;
import java.util.List;


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
	public List<LockItems> lockItem;
	
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
			operation=script.get(getIndex());
		}
		return operation;
	}
	
	public void decrementScriptPointer()
	{
		int index=getIndex();
		setIndex(index--);
	}
	
}