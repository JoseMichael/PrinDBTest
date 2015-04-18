import java.util.ArrayList;
import java.util.List;


public class WaitForGraph {
	
	int GraphTable[][];
	TestCycles obj = new TestCycles();
	
	public void initWithNumberOfScripts(int NoOfScripts)
	{
		//sets the graph table depending on the number of scripts
		GraphTable = new int [NoOfScripts][NoOfScripts];
		
		obj.initClass(NoOfScripts);
		
		//code to set all values to 0
		for(int i=0; i<GraphTable.length; i++)
			for(int j=0; j<GraphTable[i].length; j++)
				GraphTable[i][j] = 0;
	}
	
	public void setLink(int FromIndex, int ToIndex)
	{
		GraphTable[FromIndex][ToIndex] = 1;
	}
	
	public void resetNode(int NodeNo)
	{// this method is used to reset all the links FROM a Node
		for(int i=0; i<GraphTable[NodeNo].length; i++)
			GraphTable[NodeNo][i] = 0;
		
		//this piece of code resets the link that other nodes have to this node
		for(int k=0; k<GraphTable.length; k++)
		{
			if(GraphTable[k][NodeNo]==1)
				GraphTable[k][NodeNo]=0;
		}
	}
	
	//this function MIGHT be useless
	public ArrayList<Integer> findAllLinksOf(int Node)
	{//this method is used to find all the nodes that a particular node is waiting on
		ArrayList<Integer> ListOfLinks = new ArrayList<Integer>();
		for(int i=0; i<GraphTable[Node].length; i++)
		{
			if(GraphTable[Node][i]==1)
			{
				ListOfLinks.add(i);
			}
		}
		return ListOfLinks;
	}
	
	
	//this method uses Donald B. Johnson's method which can be found here http://www.cs.tufts.edu/comp/150GA/homeworks/hw1/Johnson%2075.PDF
	public ArrayList<Integer> findDeadlock()
	{
		obj.settingadjMatrix(GraphTable);
		ArrayList<Integer> listOfCycleNodes = obj.findingCycles();
		return listOfCycleNodes;
		
	}

}
