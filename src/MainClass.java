import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;
import java.util.ArrayList;

public class MainClass {

	static MMRowStorePages RS[];
	static boolean pgsUsed[];

	static String pgDates[];

	static MMRowStorePages AnsPage;

	static MetaMMR MetaRows[]; // this is used for meta data of the rows

	static int AnsPageCount = 0;

	static Calendar cal = Calendar.getInstance();

	static SimpleDateFormat sdf = new SimpleDateFormat("HHmmssSSS");

	static WriteToDisk obj = new WriteToDisk();

	static MandG colstoreobj = new MandG();

	static int numberOfPages;

	/*
	 * static public void initArrayToFindPages(int a[]) { for(int i=0;
	 * i<a.length; i++) a[i] = -1; } not needed as one bucket will only have one
	 * corresponding page
	 */

	static public int lookForPage(int pgNo, String tablename) // returns a page
																// that contain
																// a particular
																// bucket no
	{
		int PageNoWithTheBucket = -999;

		for (int i = 0; i < RS.length; i++) {
			if (MetaRows[i].actualBucketNumber == pgNo
					&& MetaRows[i].TableName == tablename) { // this checks for
																// match of both
																// bucket no and
																// table name
				PageNoWithTheBucket = i;
			}
		}
		return PageNoWithTheBucket;
	}

	static public void updateCalendar() {
		cal = Calendar.getInstance();
	}

	static public void initPages() {
		for (int i = 0; i < RS.length; i++) {
			RS[i] = new MMRowStorePages();

			MetaRows[i] = new MetaMMR();
		}
	}

	static public String setCurrentTime() {
		updateCalendar();
		String x = sdf.format(cal.getTime());
		return x;
	}

	static public void initPgDates() {
		for (int i = 0; i < pgDates.length; i++) {
			updateCalendar();
			pgDates[i] = sdf.format(cal.getTime());
		}
	}

	public int findPageWithInserts() // this funct will find if any pages has
										// inserts
	{
		for (int i = 0; i < RS.length; i++) {
			if (RS[i].UsedForInserts == 1)
				return i;
		}
		return -1;
	}

	public void logWriter(String logentry) {
		try (PrintWriter out = new PrintWriter(new BufferedWriter(
				new FileWriter("D:\\DBTest\\logfile.txt", true)))) {
			out.println(logentry);
		} catch (IOException e) {
			// exception handling left as an exercise for the reader
		}
	}

	public void flushPageToDisk(MMRowStorePages p, int pgNo) {
		// TODO add function to send data as DiskPage to disk
		// obj.insertMMRowStorePages(p);
		// p = new MMRowStorePages();
		// p.UsedForInserts=0;

		// new function
		// pgDates[pgNo] = sdf.format(cal.getTime()); need a better funct to
		// ensure page not switched
		pgDates[pgNo] = "999999999"; // this is so that this page will not be
										// switched for a while

		for (int i = 0; i < p.ID.length; i++) {
			if (p.ID[i] == null)
				break;

			int hashval = (Integer.parseInt(p.ID[i])) % 16;

			int pageNumberOfExistingPageWithHashVal = lookForPage(hashval,
					p.TableName[i]);

			if (pageNumberOfExistingPageWithHashVal != -999) {

				int pos = RS[pageNumberOfExistingPageWithHashVal].nextFreeRow();

				// fills the page with the new entry
				RS[pageNumberOfExistingPageWithHashVal].ID[pos] = p.ID[i];
				RS[pageNumberOfExistingPageWithHashVal].Name[pos] = p.Name[i];
				RS[pageNumberOfExistingPageWithHashVal].PhoneNo[pos] = p.PhoneNo[i];

				if (pos == 15) { // meaning that the page is full
					logWriter("SWAP OUT T-" + p.TableName[i] + " P-"
							+ pageNumberOfExistingPageWithHashVal + " H-"
							+ hashval);
					obj.insertMMRowStorePages(p.TableName[i],
							RS[pageNumberOfExistingPageWithHashVal],
							MetaRows[pageNumberOfExistingPageWithHashVal]); // dumps
																			// this
																			// page
					RS[pageNumberOfExistingPageWithHashVal] = new MMRowStorePages(); // code
																						// to
																						// clean
																						// dumper
																						// page
					MetaRows[pageNumberOfExistingPageWithHashVal] = new MetaMMR(); // cleans
																					// the
																					// Meta
																					// Data
					pgDates[pageNumberOfExistingPageWithHashVal] = "0"; // since
																		// this
																		// is
																		// free
																		// now
				}
			} else {
				int newPageForDump = findPageToReplace();
				RS[newPageForDump] = new MMRowStorePages();

				DiskPage IDPage = getDiskPage("ID", hashval, p.TableName[i]);
				DiskPage NamePage = getDiskPage("Name", hashval, p.TableName[i]);
				DiskPage PhonePage = getDiskPage("Phone", hashval,
						p.TableName[i]);

				logWriter("SWAP IN T-" + p.TableName[0] + " P-"
						+ newPageForDump + " B-" + hashval);

				int k;
				int l; // cuz for loop needs initializer

				// TODO check the change being made on 4/9/2015
				// first we had no.records - 14 or 0 as the initial value for
				// the page being swapped in
				// now we need to rewrite the code to ensure that only the
				// no.records%16 is in the page as old values

				int x = IDPage.numberOfRecords() % 16;
				l = IDPage.numberOfRecords() - x;

				// TODO possible bug here might be that the last record from
				// disk page is not accepted

				for (k = l; k < IDPage.numberOfRecords(); k++) // iterates and
																// fills the row
																// store page
				{
					// below its "k-l" because u want to store in array values
					// 0...n now 253...n

					RS[newPageForDump].ID[k - l] = IDPage.getValue(k);
					RS[newPageForDump].Name[k - l] = NamePage.getValue(k);
					RS[newPageForDump].PhoneNo[k - l] = PhonePage.getValue(k);
				}
				RS[newPageForDump].ID[k - l] = p.ID[i];
				RS[newPageForDump].Name[k - l] = p.Name[i];
				RS[newPageForDump].PhoneNo[k - l] = p.PhoneNo[i];

				// System.out.println("Inserted page is ");
				// RS[newPageForDump].displayRows();
				if ((k - l) == 15) { // dump the page since page is full
					MetaRows[newPageForDump].initMetaPage(hashval,
							p.TableName[i]);
					setRecordsAsOldOrNew(x, newPageForDump);

					logWriter("SWAP OUT T-" + p.TableName[i] + " P-"
							+ newPageForDump + " H-" + hashval);
					obj.insertMMRowStorePages(p.TableName[i],
							RS[newPageForDump], MetaRows[newPageForDump]); // dumps
																			// this
																			// page
					RS[newPageForDump] = new MMRowStorePages(); // code to clean
																// dumper page
					pgDates[newPageForDump] = "0";

					// clearing the meta data
					MetaRows[newPageForDump] = new MetaMMR();
				} else { // need to set meta of the page and leave it in the
							// wild
					MetaRows[newPageForDump].initMetaPage(hashval,
							p.TableName[i]);
					setRecordsAsOldOrNew(x, newPageForDump);
					pgDates[newPageForDump] = setCurrentTime();

				}

			}// end of else which is to show that a current page does not exist
				// for that bucket

		}// end of for

		// logWriter("SWAP OUT T-"+p.TableName[0]+" P-"+newPageForDump+" H-"+hashval);
		// dont see the need for this
		RS[pgNo] = new MMRowStorePages(); // code to clean initial page with
											// inserts
		RS[pgNo].UsedForInserts = 0;
		pgDates[pgNo] = "0"; // this shows that all the inserts have been
								// flushed
	}

	public void setRecordsAsOldOrNew(int num, int PgNo) {
		for (int i = 0; i < num; i++)
			MetaRows[PgNo].isNewEntry[i] = false;
	}

	public void insertIntoTable(String ID, String Name, String PhoneNo,
			String TableName) {
		// this snippet of code will either find a page with inserts or get a
		// new page
		int InsertPagePos = findPageWithInserts();
		if (InsertPagePos == -1) {
			// System.out.println("Did not find page with inserts");
			InsertPagePos = findPageToReplace();
			RS[InsertPagePos] = new MMRowStorePages();
			RS[InsertPagePos].UsedForInserts = 1;
		}

		if (RS[InsertPagePos].isEmpty()) // this page is empty?
		{
			RS[InsertPagePos].ID[0] = ID;
			RS[InsertPagePos].Name[0] = Name;
			RS[InsertPagePos].PhoneNo[0] = PhoneNo;
			RS[InsertPagePos].TableName[0] = TableName;
		} else {
			int i = 0;
			while (RS[InsertPagePos].ID[i] != null)
				i++;
			RS[InsertPagePos].ID[i] = ID;
			RS[InsertPagePos].Name[i] = Name;
			RS[InsertPagePos].PhoneNo[i] = PhoneNo;
			RS[InsertPagePos].TableName[i] = TableName;
			if (i == 15) {
				// page is full so flush to disk
				System.out.println("Inside flush to disk");
				sortInsertPage(RS[InsertPagePos]);
				flushPageToDisk(RS[InsertPagePos], InsertPagePos);
				pgDates[InsertPagePos] = "0"; // sets the timestamp of this page
												// to 0
				// thus the page is shown as least recently used and now anyone
				// can use it
			}
		}
		// flushPageToDisk(RS[InsertPagePos],InsertPagePos); //TODO maybe change
		// this
		// pgDates[InsertPagePos] = "0"; //del these last 2 lines
		// thus now it wont flush after every insert
		logWriter("Inserted : " + ID + ", " + Name + ", " + PhoneNo);
	}

	public void sortInsertPage(MMRowStorePages p) {
		// we are sorting the values in the hopes that the accesses to the same
		// bucket page will be more convinient

		int ArrayOfHashValues[] = new int[16];

		for (int i = 0; i < 16; i++) {
			ArrayOfHashValues[i] = (Integer.parseInt(p.ID[i])) % 16;
		}

		for (int k = 0; k < 16; k++)
			for (int j = k + 1; j < 16; j++) {
				if (ArrayOfHashValues[j] < ArrayOfHashValues[k]) {
					int temp = ArrayOfHashValues[k];
					String tempID = p.ID[k];
					String tempName = p.Name[k];
					String tempPhNo = p.PhoneNo[k];
					String tempTableName = p.TableName[k];

					// switching values via classic sort
					ArrayOfHashValues[k] = ArrayOfHashValues[j];
					p.ID[k] = p.ID[j];
					p.Name[k] = p.Name[j];
					p.PhoneNo[k] = p.PhoneNo[j];
					p.TableName[k] = p.TableName[j];

					ArrayOfHashValues[j] = temp;
					p.ID[j] = tempID;
					p.Name[j] = tempName;
					p.PhoneNo[j] = tempPhNo;
					p.TableName[j] = tempTableName;
				}
			}

	} // end of function for sorting insert page

	public int findPageToReplace() {
		int low = 0;
		for (int i = 0; i < RS.length; i++) {
			int time1 = Integer.parseInt(pgDates[i]);
			int time2 = Integer.parseInt(pgDates[low]);
			if (time1 < time2) {
				low = i;
			}
		}

		// the following lines of code ensures that if the page found for
		// swapping
		// is one where inserts have taken place then that page, before being
		// discarded
		// is flushed into the disk
		if (RS[low].UsedForInserts == 1)
			flushPageToDisk(RS[low], low);

		// lines of code to ensure that pages with dirty data are flushed into
		// disks
		if (MetaRows[low].actualBucketNumber != -999) {
			int hashval = MetaRows[low].actualBucketNumber;
			logWriter("SWAP OUT T-" + MetaRows[low].TableName + " P-" + low
					+ " H-" + hashval);
			obj.insertMMRowStorePages(MetaRows[low].TableName, RS[low],
					MetaRows[low]); // dumps this page
			RS[low] = new MMRowStorePages(); // code to clean dumper page
			MetaRows[low] = new MetaMMR(); // cleans the Meta Data
			pgDates[low] = "0"; // since this is free now
		}

		return low;
	}

	public DiskPage getDiskPage(String AttributeName, int BucketNumber,
			String TableName) {
		DiskPage page = new DiskPage();

		List<String> list = obj.getDiskPage(TableName, AttributeName,
				BucketNumber);

		// assigning data to DiskPage page
		page.makeDiskPage(list);

		return page;
	}

	int recordCounter; // counts the number of records that have been filled

	public void fillAnsPageEntry(String ID, String Name, String PhoneNo) {
		AnsPage.ID[AnsPageCount] = ID;
		AnsPage.Name[AnsPageCount] = Name;
		AnsPage.PhoneNo[AnsPageCount] = PhoneNo;
		AnsPageCount++;
	}

	public void fillPage(MMRowStorePages p, DiskPage ID, DiskPage Name,
			DiskPage PhoneNo, int IDval, int pageNumber) {

		if (p.isEmpty()) {
			for (int i = 0; i < 16; i++) {
				if ((recordCounter + i) >= ID.numberOfRecords())
					break;
				p.ID[i] = ID.getValue(recordCounter + i);
				p.Name[i] = Name.getValue(recordCounter + i);
				p.PhoneNo[i] = PhoneNo.getValue(recordCounter + i);
				if (p.ID[i].equals(String.valueOf(IDval))) // if the ID matches
				{
					fillAnsPageEntry(p.ID[i], p.Name[i], p.PhoneNo[i]);
				}
			}
			recordCounter = recordCounter + 16;
		}
		updateCalendar();
		pgDates[pageNumber] = sdf.format(cal.getTime());
	}

	public void retrieveWithID(int IDval, String TableName) // IDval is set as
															// int here
	{
		recordCounter = 0;
		AnsPage = new MMRowStorePages();
		AnsPageCount = 0;

		int hashVal = IDval % 16;
		DiskPage pageWithID = getDiskPage("ID", hashVal, TableName);
		int numberOfRecords = pageWithID.numberOfRecords(); // this is used to
															// find no. of
															// records in that
															// page
		DiskPage pageWithName = getDiskPage("Name", hashVal, TableName);
		DiskPage pageWithNumber = getDiskPage("Phone", hashVal, TableName);

		while (numberOfRecords > recordCounter) {
			int initPage = findPageToReplace();
			/*
			 * if(initPage>RS.length) break; //this is used to ensure that once
			 * all the pages are full the code exits this piece of code was made
			 * redundant as I used time to swap pages
			 */

			RS[initPage] = new MMRowStorePages();
			logWriter("SWAP IN T-" + TableName + " P-" + initPage + " B-"
					+ hashVal);
			fillPage(RS[initPage], pageWithID, pageWithName, pageWithNumber,
					IDval, initPage);
			// initPage ++;

		}
		int AnsPagePos = findPageToReplace(); // this var will decide where the
												// ans page will reside
		RS[AnsPagePos] = AnsPage;
		updateCalendar();
		pgDates[AnsPagePos] = sdf.format(cal.getTime());
		AnsPage.displayRows();
		if (AnsPage.ID[0] == null)
			logWriter("Aborted Transaction");
		else
			logWriter("Read : " + AnsPage.ID[0] + ", " + AnsPage.Name[0] + ", "
					+ AnsPage.PhoneNo[0]);

	}

	public static void main(String agrs[]) throws IOException {
		// used to take size of buffer and allocate space for row store pages
		System.out.println("Please enter the size of the buffer");
		System.out.println("-----------------------------------");
		Scanner sc = new Scanner(System.in);
		int buffersize = sc.nextInt();
		
		System.out.println();
		
		System.out.println("Please enter total number of scripts to use");
		System.out.println("-------------------------------------------");
		int nmbrofscripts = sc.nextInt();
		
		System.out.println();
		
		List<String> filepaths = new ArrayList<String>();
		for(int i = 0; i < nmbrofscripts; i++)
		{
			System.out.println("Please enter the file path for script " + i);
			System.out.println("---------------------------------------");
			filepaths.add(sc.nextLine());
			System.out.println();
		}
		
		System.out.println("Please select concurrency reading method");
		System.out.println("----------------------------------------");
		System.out.println("1 - Round Robin");
		System.out.println("2 - Random");
		System.out.println("----------------------------------------");
		System.out.print("Enter a number: ");
		int rdmethod = sc.nextInt();
		
		System.out.println();
		
		int rngseed = 0;
		if (rdmethod == 2)
		{
			System.out.println("Please enter a seed for the RNG");
			System.out.println("-------------------------------");
			rngseed = sc.nextInt();
		}
		
		numberOfPages = ((buffersize) / 512) / 2;
		RS = new MMRowStorePages[numberOfPages];
		pgsUsed = new boolean[numberOfPages];
		pgDates = new String[numberOfPages];
		MetaRows = new MetaMMR[numberOfPages];

		initPgDates();
		initPages();

		// setting pages used to false
		for (int i = 0; i < pgsUsed.length; i++)
			pgsUsed[i] = false;


		File meta = new File(obj.metadataFile);
		if (meta.exists()) {
			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(meta));
				String line;
				try {
					while ((line = br.readLine()) != null) {

						String[] temp = line.split("-");
						if (obj.tableName.contains(temp[0].toUpperCase())) {

						} else {
							obj.tableName.add(temp[0].toUpperCase());
						}
					}

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			int fileCount;
			while (true) {
				System.out.println("Please enter the count of data files:");
				fileCount = sc.nextInt();
				if (fileCount > 0)
					break;
			}

			List<String> fileList = new ArrayList<String>();
			for (int i = 0; i < fileCount; i++) {
				fileList.add(sc.next());
			}
			obj.files = fileList;

			for (String iterator : fileList) {
				String[] nameArray = iterator.split(".txt");

				String fileId = obj.baseFileLoc + iterator + obj.id;
				String fileName = obj.baseFileLoc + iterator + obj.name;
				String filePhone = obj.baseFileLoc + iterator + obj.phone;

				File fid = new File(fileId);
				File fname = new File(fileName);
				File fphone = new File(filePhone);
				if (fid.exists())
					fid.delete();
				if (fname.exists())
					fname.delete();
				if (fphone.exists())
					fphone.delete();

				if (!obj.tableName.contains(nameArray[0].toUpperCase())) {
					obj.newList = obj.readFile(iterator);
					obj.tableName.add(nameArray[0].toUpperCase());
					obj.formHeaders(nameArray[0].toUpperCase());
					obj.insertData(nameArray[0].toUpperCase());
				}
				System.out.println("\nTableCount:" + obj.tableName.size());
			}

		}
		MainClass rclass = new MainClass();
		// rclass.retrieveWithID(139);
		rclass.scriptAnalyzer();
		// closing scanner
		sc.close();
	}
}