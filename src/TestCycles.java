import java.util.ArrayList;
import java.util.List;


/**
 * Testfile for elementary cycle search.
 *
 * @author Frank Meyer
 *
 */
public class TestCycles {

	/**
	 * @param args
	 */
	
		String nodes[];
		boolean adjMatrix[][];

		
		public void initClass(int NoOfNodes)
		{
			 nodes = new String[NoOfNodes];
			 adjMatrix = new boolean[NoOfNodes][NoOfNodes];
			
			for (int i2 = 0; i2 < 10; i2++) {
				nodes[i2] =  ""+i2;
			}
			
		}

		/*adjMatrix[0][1] = true;
		adjMatrix[1][2] = true;
		adjMatrix[2][0] = true;
		adjMatrix[2][4] = true;
		adjMatrix[1][3] = true;
		adjMatrix[3][6] = true;
		adjMatrix[6][5] = true;
		adjMatrix[5][3] = true;
		adjMatrix[6][7] = true;
		adjMatrix[7][8] = true;
		adjMatrix[7][9] = true;
		adjMatrix[9][6] = true;*/
		
		public void settingadjMatrix(int a[][])
		{
			
			for(int i=0; i<a.length; i++)
				for(int j=0; j<a[i].length; j++)
				{
					if(a[i][j]==1)
						adjMatrix[i][j] = true;
				}
		
        /*adjMatrix[0][1] = true;
        adjMatrix[1][2] = true;
        adjMatrix[2][0] = true; adjMatrix[2][6] = true;
        adjMatrix[3][4] = true;
        adjMatrix[4][5] = true; adjMatrix[4][6] = true;
        adjMatrix[5][3] = true;
        adjMatrix[6][7] = true;
        adjMatrix[7][8] = true;
        adjMatrix[8][6] = true;
        
        adjMatrix[6][1] = true;
        */
        
		}
		
		public ArrayList<Integer> findingCycles()
		{
			
		ArrayList<Integer> ListToReturn = new ArrayList<Integer>();;
		ElementaryCyclesSearch ecs = new ElementaryCyclesSearch(adjMatrix, nodes);
		List cycles = ecs.getElementaryCycles();
		for (int i = 0; i < cycles.size(); i++) {
			List cycle = (List) cycles.get(i);
			ListToReturn = new ArrayList<Integer>();
			for (int j = 0; j < cycle.size(); j++) {
				String node = (String) cycle.get(j);
				if (j < cycle.size() - 1) {
					System.out.print(node + " -> ");
					ListToReturn.add(Integer.parseInt(node));
				} else {
					System.out.print(node);
					ListToReturn.add(Integer.parseInt(node));
				}
			}
			System.out.print("\n");
			if(!ListToReturn.isEmpty())
				return ListToReturn;
		}
		return ListToReturn;
		
	}

}