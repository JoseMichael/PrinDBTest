import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class test2 {

	public static void main(String agrs[]) throws IOException {
		File script = new File("C:\\Users\\JoseMichael\\Desktop\\DB\\test2.txt");
		Scanner fileScanner = new Scanner(script);
		while (fileScanner.hasNextLine()) {
			String line = fileScanner.nextLine();

			Scanner lineScanner = new Scanner(line);
			while (lineScanner.hasNext()) {
				String token = lineScanner.next();
				if (token.equals("R")) {
					String tableName = lineScanner.next(); // this is for table
															// name
					String test = lineScanner.next(); // this is for ID
					// retrieveWithID(Integer.parseInt(test));
				} else if (token.equals("I")) {
					String test = lineScanner.next();
					// test = lineScanner.next();
					String ID = lineScanner.next(); // this gives ID
					ID = ID.substring(1, ID.length() - 1);
					// test = lineScanner.next();
					String Name = lineScanner.next(); // this gives Name
					Name = Name.substring(0, Name.length() - 1);
					// test = lineScanner.next();
					String PhoneNo = lineScanner.next(); // this gives phone no
					PhoneNo = PhoneNo.substring(0, PhoneNo.length() - 1);

					// insertIntoTable( ID, Name, PhoneNo);
				}
				// System.out.println(token);
				// do whatever needs to be done with token
			}
			lineScanner.close();
			// you're at the end of the line here. Do what you have to do.
		}
		fileScanner.close();
	}
}
