import java.util.ArrayList;
import java.util.List;


/**
 * @author Priyanka
 *
 */
public class TransProc {
	
	List<String> script=new ArrayList<>();
	List<String> completedOperations=new ArrayList<>();
	int index=-1;
	int scriptNum;
	boolean isWaiting=false;
	//TODO: GET FILESIZE
	int filesize=-1;
	boolean isProcess=false;
	
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