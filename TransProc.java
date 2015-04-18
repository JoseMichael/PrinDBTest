import java.util.ArrayList;
import java.util.List;


/**
 * @author Priyanka
 *
 */
public class TransProc {
	
	private List<String> script=new ArrayList<>();
	private List<String> completedOperations=new ArrayList<>();
	private int index=-1;
	private int scriptNum;
	private boolean isWaiting=false;
	//TODO: GET FILESIZE
	private int filesize=-1;
	private boolean isProcess=false;
	private LockItems lockItem;
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
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
	
}