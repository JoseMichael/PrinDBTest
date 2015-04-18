import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ScriptAnalyzer
{
	public List<String> scriptAnalyzer(TransProc tp) throws IOException {
		List<String> operationList = new ArrayList<String>();

		Scanner lineScanner = new Scanner(tp.script.get(tp.index));
		while (lineScanner.hasNext()) {
			String token = lineScanner.next();
			operationList.add(token);
			if (token.equals("R")) {
				String tableName = lineScanner.next(); // this is for table name
				String test = lineScanner.next(); // this is for ID
				operationList.add(tableName);
				operationList.add(test);
			} else if (token.equals("I")) {
				String tableName = lineScanner.next();
				String ID = lineScanner.next(); // this gives ID
				ID = ID.substring(1, ID.length() - 1);
				String Name = lineScanner.next(); // this gives Name
				Name = Name.substring(0, Name.length() - 1);
				String PhoneNo = lineScanner.next(); // this gives phone no
				PhoneNo = PhoneNo.substring(0, PhoneNo.length() - 1);
				operationList.add(tableName);
				operationList.add(ID);
				operationList.add(Name);
				operationList.add(PhoneNo);
			} else if (token.equals("G")) {
				String tableName = lineScanner.next();
				String AreaCode = lineScanner.next();
				operationList.add(tableName);
				operationList.add(AreaCode);
			} else if (token.equals("M")) {
				String tableName = lineScanner.next();
				String AreaCode = lineScanner.next();
				operationList.add(tableName);
				operationList.add(AreaCode);
			} else if (token.equals("D")) {
				String tableName = lineScanner.next();
				operationList.add(tableName);
			} else if (token.equals("B")) {
				String emode = lineScanner.next();
				operationList.add(emode);
			} else if (token.equals("A")) {
				
			} else if (token.equals("C")) {
				
			}
			// System.out.println(token);
			// do whatever needs to be done with token
		}
		lineScanner.close();
		// you're at the end of the line here. Do what you have to do.
		return operationList;
	}
}