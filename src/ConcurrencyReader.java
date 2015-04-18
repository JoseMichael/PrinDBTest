import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class ConcurrencyReader
{
	public List<List<String>> roundRobinRead(List<TransProc> tplist)
	{
		ScriptAnalyzer sc = new ScriptAnalyzer();
		List<List<String>> opList = new ArrayList<List<String>>();
		for (int i = 0; i < tplist.size(); i++)
		{
			List<String> dummyList = new ArrayList<String>();
			
			//Need a try block, not sure why
			try {
				dummyList = new ArrayList<String>(sc.scriptAnalyzer(tplist.get(i)));
				opList.add(dummyList);
				tplist.get(i).index = tplist.get(i).index + 1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return opList;
	}
	
	public List<List<List<String>>> randomRead(List<TransProc> tplist, int rndseed)
	{
		ScriptAnalyzer sc = new ScriptAnalyzer();
		
		//Setup is a List of a List of strings FOR EACH transproc
		//Use for the returned object would be:
		//oplist.get(index_of_desired_tp).get(operation_index).get(string_of_operation_index)
		//e.g. if I want the 2nd operation from the first tp:
		//for(int i = 0; i < oplist.get(0).get(1).size();i++)
		//{
		//  oplist.get(0).get(1).get(i);
		//}
		//NOTE: TO GET THE LIST IN THE SAME ORDER AS THE SET BELOW, WILL NEED TO
		//REIMPLEMENT THE SAME CODE WHERE NEEDED; TODO: CREATE A METHOD FOR IT
		//
		List<List<List<String>>> oplist = new ArrayList<List<List<String>>>();
		
		////////////////////////////////////////////////////////////////////
		// CODE TO GET RANDOMLY ORDERED LIST BASED ON SIZE OF TRANSPROC LIST
		int usrseed = rndseed;
		Random rng = new Random(usrseed);
		Set<Integer> intset = new LinkedHashSet<Integer>();
		int listsize = tplist.size();
		while(intset.size() < listsize)
		{
			int next = rng.nextInt(listsize);
			intset.add(next);
		}
		
		List<Integer> intlist = new ArrayList<Integer>();
		intlist.addAll(intset);
		// END ORDERED LIST CODE
		////////////////////////////////////////////////////////////////////
		
		for (int i = 0; i < tplist.size(); i++)
		{
			// A list of a list of strings for a given tp
			List<List<String>> tpdummylist = new ArrayList<List<String>>();
			
			//Choose a random number between 1 and remaining lines in file
			//NOTE: Math should be right, check here if errors start occurring though
			int linestopull = rng.nextInt(tplist.get(intlist.get(i)).filesize - tplist.get(intlist.get(i)).index);
			
			
			//Need a try block, not sure why
			try
			{
				List<String> opdummylist = new ArrayList<String>();
				for(int z = 0; z < linestopull; z++)
				{
					opdummylist = new ArrayList<String>(sc.scriptAnalyzer(tplist.get(intlist.get(i))));
					tpdummylist.add(opdummylist);
				}
			} 
			catch (IOException e)
			{
				e.printStackTrace();
			}
			oplist.add(tpdummylist);
		}
		return oplist;
	}
}
