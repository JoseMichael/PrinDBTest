import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class MandG {
	WriteToDisk wtd = new WriteToDisk();
	Map<Integer, PageColumnMeta> MMPAGELIST = new HashMap<Integer, PageColumnMeta>();

	// Given a table id and an area code, return a list of records
	// with phone numbers containing the area code
	@SuppressWarnings("unused")
	public void MQuery(String tableid, String areacode, int MMPAGES,
			WriteToDisk obj) {
		wtd = obj;
		// Write Operation to Logfile
		String operation = "M " + tableid + " " + areacode;
		logWriter(operation);

		// Create lists of datalocation for each answer list, as well as an
		// answer row for final output
		List<DataLocation> answerList = new ArrayList<DataLocation>();
		List<DataLocation> answerListName = new ArrayList<DataLocation>();
		List<DataLocation> answerListId = new ArrayList<DataLocation>();
		List<Schema> answerRows = new ArrayList<Schema>();

		// List of slotted pages, each with a list of strings from that page
		// (phone numbers in this case)
		HashMap<Integer, List<String>> slottedListPhone = new HashMap<>(
				wtd.getSlottedList(tableid, "Phone"));

		// List of pages to be passed into main memory
		List<PageColumnMeta> pageListPhone = new ArrayList<PageColumnMeta>();

		// keep track of total size of string in page
		int totalstringsize = 0;

		// Create dummy vars to hold data before actually inserting
		List<DataLocation> dummyLocs = new ArrayList<DataLocation>();
		PageColumnMeta dummyMeta = new PageColumnMeta();

		// For each key (bucket) go through the list and separate into pages of
		// size 512
		for (int key : slottedListPhone.keySet()) {
			List<String> listOfAllSlottedPageEntries = slottedListPhone
					.get(key);
			for (int i = 0; i < listOfAllSlottedPageEntries.size(); i++) {
				// Create dummy to insert into Pages
				DataLocation dataL = new DataLocation();

				// if size after adding is less than or eq to 512, add the word
				// to dummypage
				if ((totalstringsize + listOfAllSlottedPageEntries.get(i)
						.length()) <= 512) {
					dataL.setTableName(tableid);
					dataL.setBucketNo(key);
					dataL.setRowNo(i);
					dataL.setDataString(listOfAllSlottedPageEntries.get(i));
					dataL.setDataType("phone");
					dummyLocs.add(dataL);
				}
				// if over 512, insert the list of dummyLocs into a page
				else {
					dummyMeta.setData(dummyLocs);
					pageListPhone.add(dummyMeta);
					totalstringsize = 0;

					// Create new dummies
					dummyMeta = new PageColumnMeta();
					dummyLocs = new ArrayList<DataLocation>();

					// Catch the data that we don't insert into the added page
					dataL.setTableName(tableid);
					dataL.setBucketNo(key);
					dataL.setRowNo(i);
					dataL.setDataString(listOfAllSlottedPageEntries.get(i));
					dataL.setDataType("phone");
					dummyLocs.add(dataL);
				}
				totalstringsize += listOfAllSlottedPageEntries.get(i).length();
			}
		}

		// Insert any leftover data
		if (totalstringsize != 0) {
			dummyMeta.setData(dummyLocs);
			pageListPhone.add(dummyMeta);
		}

		// Bring in one page at a time
		for (int x = 0; x <= pageListPhone.size() - 1; x++) {
			// Assign Meta Data for each page brought in
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ssSSS");
			PageColumnMeta pcm = new PageColumnMeta();
			pcm.setPageId(x);
			pcm.setTimeStamp(sdf.format(cal.getTime()));
			pcm.setData(pageListPhone.get(x).getData());

			// Sleep to be sure the timestamps are different
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			int index = 0;
			// Check if we have max pages in MM, if we do, get rid of LRU based
			// on TimeStamp
			if (MMPAGELIST.size() == MMPAGES) {
				// Find the index of the page to replace
				index = CalcPageToReplace(MMPAGELIST);

				String pageswapout = "SWAP OUT T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pageswapout);

				// Overwriting LRU
				MMPAGELIST.put(index, pcm);

				String pageswapin = "SWAP IN T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pageswapin);
			} else {
				index = MMPAGELIST.size();
				MMPAGELIST.put(index, pcm);

				String pagecreate = "CREATE T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pagecreate);
			}
			// Search the page we just put into main memory for our area code
			// and
			List<DataLocation> dummyAnswerList = new ArrayList<DataLocation>(
					searchPage(MMPAGELIST.get(index).getData(), areacode));
			for (DataLocation dl : dummyAnswerList) {
				answerList.add(dl);
			}
		}

		// ////////////////////////////////////////////////////////////////////////
		// ///// We now have a list of answers that match our query
		// ///////////////
		// ////////////////////////////////////////////////////////////////////////
		List<DataLocation> uniqueAnswerList = new ArrayList<DataLocation>();

		List<PageColumnMeta> pageListName = new ArrayList<PageColumnMeta>();

		// Create dummy vars to hold data before actually inserting
		List<DataLocation> dummyLocsName = new ArrayList<DataLocation>();
		PageColumnMeta dummyMetaName = new PageColumnMeta();

		int pageno = 0;

		int repeatcheckflag = 0;
		List<String> repeatTest = new ArrayList<String>();
		List<String> slottedPage = new ArrayList<String>();

		totalstringsize = 0;
		for (DataLocation dl : answerList) {
			// ADDED REPEATCHECKFLAG TO DETERMINE
			// IF THE NEXT PAGE IS A REPEAT
			if (repeatcheckflag == 1) {
				List<String> testList = wtd.getSlottedPageForBucketId(
						dl.getTableName(), "Name", dl.getBucketNo(),
						dl.getRowNo());
				if (repeatTest.equals(testList)) {
					// THIS IS A REPEAT, DO NOTHING
					continue;
				}
			}
			// gets us the specific slotted page for the bucket we want
			slottedPage = new ArrayList<>(wtd.getSlottedPageForBucketId(
					dl.getTableName(), "Name", dl.getBucketNo(), dl.getRowNo()));

			// populate repeat test vars
			repeatTest = new ArrayList<>(slottedPage);
			repeatcheckflag = 1;

			// calculate pageno using the datalocation in answerlist, but with
			// data size of "name"
			pageno = getPageForDT(dl, "name");

			for (int i = 0; i < slottedPage.size(); i++) {
				// Create dummy to insert into Pages
				DataLocation dataL = new DataLocation();

				// if size after adding is less than or eq to 512, add the word
				// to dummypage
				if ((totalstringsize + slottedPage.get(i).length()) <= 512) {
					// assign variables for dummy location
					dataL.setTableName(tableid);
					dataL.setBucketNo(dl.getBucketNo());
					// set row to be the current iterating index + the data size
					// * the pageno we're on
					dataL.setRowNo(i + (31 * pageno));
					dataL.setDataString(slottedPage.get(i));
					dataL.setDataType("name");
					// assign the dummy location to a dummy list
					dummyLocsName.add(dataL);
				}
				// if over 512, insert the list of dummyLocs into a page
				else {
					// assign dummylist to a page
					dummyMetaName.setData(dummyLocsName);
					pageListName.add(dummyMetaName);
					totalstringsize = 0;

					// Create new dummies
					dummyMetaName = new PageColumnMeta();
					dummyLocsName = new ArrayList<DataLocation>();

					// Catch the data that we don't insert into the added page
					dataL.setTableName(tableid);
					dataL.setBucketNo(dl.getBucketNo());
					dataL.setRowNo(i + (31 * pageno));
					dataL.setDataString(slottedPage.get(i));
					dataL.setDataType("name");
					dummyLocsName.add(dataL);
				}
				totalstringsize += slottedPage.get(i).length();
			}
		}

		// Insert any leftover data
		if (totalstringsize != 0) {
			dummyMetaName.setData(dummyLocsName);
			pageListName.add(dummyMetaName);
		}

		// Bring in one page at a time
		for (int x = 0; x <= pageListName.size() - 1; x++) {
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ssSSS");
			PageColumnMeta pcm = new PageColumnMeta();
			pcm.setPageId(x);
			pcm.setTimeStamp(sdf.format(cal.getTime()));
			pcm.setData(pageListName.get(x).getData());

			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			int index = 0;
			// Check if we have max pages in MM, if we do, get rid of LRU based
			// on TimeStamp
			if (MMPAGELIST.size() == MMPAGES) {
				// Find the index of the page to replace
				index = CalcPageToReplace(MMPAGELIST);

				String pageswapout = "SWAP OUT T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pageswapout);

				// Overwriting LRU
				MMPAGELIST.put(index, pcm);

				String pageswapin = "SWAP IN T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pageswapin);
			} else {
				index = MMPAGELIST.size();
				MMPAGELIST.put(index, pcm);

				String pagecreate = "CREATE T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pagecreate);
			}
			// Search the page we just put into main memory based on our answer
			// list
			List<DataLocation> dummyAnswerList = new ArrayList<DataLocation>(
					searchForName(MMPAGELIST.get(index).getData(), answerList));
			for (DataLocation dl : dummyAnswerList) {
				answerListName.add(dl);
			}
		}

		// ////////////////////////////////////////////////////////////////////////
		// ///// We now have a list of names that match our query
		// /////////////////
		// ////////////////////////////////////////////////////////////////////////

		List<PageColumnMeta> pageListId = new ArrayList<PageColumnMeta>();

		// Create dummy vars to hold data before actually inserting
		List<DataLocation> dummyLocsId = new ArrayList<DataLocation>();
		PageColumnMeta dummyMetaId = new PageColumnMeta();

		totalstringsize = 0;
		repeatcheckflag = 0;
		repeatTest = new ArrayList<String>();
		slottedPage = new ArrayList<String>();

		for (DataLocation dl : answerList) {
			// ADDED REPEATCHECKFLAG TO DETERMINE
			// IF THE NEXT PAGE IS A REPEAT
			if (repeatcheckflag == 1) {
				List<String> testList = wtd.getSlottedPageForBucketId(
						dl.getTableName(), "Id", dl.getBucketNo(),
						dl.getRowNo());
				if (repeatTest.equals(testList)) {
					// THIS IS A REPEAT, DO NOTHING
					continue;
				}
			}
			// gets us the specific slotted page for the bucket we want
			slottedPage = new ArrayList<>(wtd.getSlottedPageForBucketId(
					dl.getTableName(), "Id", dl.getBucketNo(), dl.getRowNo()));
			repeatTest = new ArrayList<>(slottedPage);
			repeatcheckflag = 1;

			pageno = getPageForDT(dl, "id");
			for (int i = 0; i < slottedPage.size(); i++) {
				// Create dummy to insert into Pages
				DataLocation dataL = new DataLocation();

				// if size after adding is less than or eq to 512, add the word
				// to dummypage
				if ((totalstringsize + slottedPage.get(i).length()) <= 512) {
					dataL.setTableName(tableid);
					dataL.setBucketNo(dl.getBucketNo());
					dataL.setRowNo(i + (125 * pageno));
					dataL.setDataString(slottedPage.get(i));
					dataL.setDataType("id");
					dummyLocsId.add(dataL);
				}
				// if over 512, insert the list of dummyLocs into a page
				else {
					dummyMetaId.setData(dummyLocsId);
					pageListId.add(dummyMetaId);
					totalstringsize = 0;

					// Create new dummies
					dummyMetaId = new PageColumnMeta();
					dummyLocsId = new ArrayList<DataLocation>();

					// Catch the data that we don't insert into the added page
					dataL.setTableName(tableid);
					dataL.setBucketNo(dl.getBucketNo());
					dataL.setRowNo(i + (125 * pageno));
					dataL.setDataString(slottedPage.get(i));
					dataL.setDataType("id");
					dummyLocsId.add(dataL);
				}
				totalstringsize += slottedPage.get(i).length();
			}
		}

		// Insert any leftover data
		if (totalstringsize != 0) {
			dummyMetaId.setData(dummyLocsId);
			pageListId.add(dummyMetaId);
		}

		// Bring in one page at a time
		for (int x = 0; x <= pageListId.size() - 1; x++) {
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ssSSS");
			PageColumnMeta pcm = new PageColumnMeta();
			pcm.setPageId(x);
			pcm.setTimeStamp(sdf.format(cal.getTime()));
			pcm.setData(pageListId.get(x).getData());

			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			int index = 0;
			// Check if we have max pages in MM, if we do, get rid of LRU based
			// on TimeStamp
			if (MMPAGELIST.size() == MMPAGES) {
				// Find the index of the page to replace
				index = CalcPageToReplace(MMPAGELIST);

				String pageswapout = "SWAP OUT T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pageswapout);

				// Overwriting LRU
				MMPAGELIST.put(index, pcm);

				String pageswapin = "SWAP IN T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pageswapin);
			} else {
				index = MMPAGELIST.size();
				MMPAGELIST.put(index, pcm);

				String pagecreate = "CREATE T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pagecreate);
			}
			// Search the page we just put into main memory based on our answer
			// list
			List<DataLocation> dummyAnswerList = new ArrayList<DataLocation>(
					searchForId(MMPAGELIST.get(index).getData(), answerList));
			for (DataLocation dl : dummyAnswerList) {
				answerListId.add(dl);
			}
		}
		// ////////////////////////////////////////////////////////////////////////
		// ///// We now have a list of ids that match our query
		// ///////////////////
		// ////////////////////////////////////////////////////////////////////////

		// Output our answer
		for (int i = 0; i < answerList.size(); i++) {
			String output = "MRead: " + answerListId.get(i).getDataString()
					+ "," + answerListName.get(i).getDataString() + ","
					+ answerList.get(i).getDataString();
			logWriter(output);
			System.out.println(output);
		}

		if (answerList.size() == 0) {
			String output = "No matches found for area code: "
					+ areacode.toString();
			logWriter(output);
			System.out.println(output);
		}
	}

	public void GQuery(String tableid, String areacode, int MMPAGES,
			WriteToDisk obj) {
		wtd = obj;

		String operation = "G " + tableid + " " + areacode;
		logWriter(operation);

		int runningcount = 0;

		// List of slotted pages, each with a list of strings from that page
		// (phone numbers in this case)
		HashMap<Integer, List<String>> slottedListPhone = new HashMap<>(
				wtd.getSlottedList(tableid, "Phone"));

		// List of pages to be passed into main memory
		List<PageColumnMeta> pageListPhone = new ArrayList<PageColumnMeta>();

		// keep track of total size of string in page
		int totalstringsize = 0;

		// Create dummy vars to hold data before actually inserting
		List<DataLocation> dummyLocs = new ArrayList<DataLocation>();
		PageColumnMeta dummyMeta = new PageColumnMeta();

		for (int key : slottedListPhone.keySet()) {
			List<String> listOfAllSlottedPageEntries = slottedListPhone
					.get(key);
			for (int i = 0; i < listOfAllSlottedPageEntries.size(); i++) {
				// Create dummy to insert into Pages
				DataLocation dataL = new DataLocation();

				// if size after adding is less than or eq to 512, add the word
				// to dummypage
				if ((totalstringsize + listOfAllSlottedPageEntries.get(i)
						.length()) <= 512) {
					dataL.setTableName(tableid);
					dataL.setBucketNo(key);
					dataL.setRowNo(i);
					dataL.setDataString(listOfAllSlottedPageEntries.get(i));
					dataL.setDataType("phone");
					dummyLocs.add(dataL);
				}
				// if over 512, insert the list of dummyLocs into a page
				else {
					dummyMeta.setData(dummyLocs);
					pageListPhone.add(dummyMeta);
					totalstringsize = 0;

					// Create new dummies
					dummyMeta = new PageColumnMeta();
					dummyLocs = new ArrayList<DataLocation>();

					// Catch the data that we don't insert into the added page
					dataL.setTableName(tableid);
					dataL.setBucketNo(key);
					dataL.setRowNo(i);
					dataL.setDataString(listOfAllSlottedPageEntries.get(i));
					dataL.setDataType("phone");
					dummyLocs.add(dataL);
				}
				totalstringsize += listOfAllSlottedPageEntries.get(i).length();
			}
		}

		// Insert any leftover data
		if (totalstringsize != 0) {
			dummyMeta.setData(dummyLocs);
			pageListPhone.add(dummyMeta);
		}

		// Bring in one page at a time
		for (int x = 0; x <= pageListPhone.size() - 1; x++) {
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ssSSS");
			PageColumnMeta pcm = new PageColumnMeta();
			pcm.setPageId(x);
			pcm.setTimeStamp(sdf.format(cal.getTime()));
			pcm.setData(pageListPhone.get(x).getData());

			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			int index = 0;
			// Check if we have max pages in MM, if we do, get rid of LRU based
			// on TimeStamp
			if (MMPAGELIST.size() == MMPAGES) {
				// Find the index of the page to replace
				index = CalcPageToReplace(MMPAGELIST);

				String pageswapout = "SWAP OUT T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pageswapout);

				// Overwriting LRU
				MMPAGELIST.put(index, pcm);

				String pageswapin = "SWAP IN T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pageswapin);
			} else {
				index = MMPAGELIST.size();
				MMPAGELIST.put(index, pcm);

				String pagecreate = "CREATE T-"
						+ MMPAGELIST.get(index).getData().get(0).getTableName()
						+ " P-"
						+ Integer.toString(getPage(MMPAGELIST.get(index)
								.getData().get(0)))
						+ " B-"
						+ Integer.toString(MMPAGELIST.get(index).getData()
								.get(0).getBucketNo());
				logWriter(pagecreate);
			}
			// Search the page we just put into main memory for our area code
			// and keep a running count
			runningcount += countPage(MMPAGELIST.get(index).getData(), areacode);
		}
		String output = "GCount: " + Integer.toString(runningcount);
		logWriter(output);
		System.out.println(output);
	}

	@SuppressWarnings("rawtypes")
	public int CalcPageToReplace(Map<Integer, PageColumnMeta> mypages) {
		Iterator i = mypages.entrySet().iterator();

		Map.Entry mypair = (Map.Entry) i.next();
		PageColumnMeta LRU = new PageColumnMeta(
				(PageColumnMeta) mypair.getValue());
		int LRUindex = (int) mypair.getKey();

		while (i.hasNext()) {
			mypair = (Map.Entry) i.next();
			PageColumnMeta swap = new PageColumnMeta(
					(PageColumnMeta) mypair.getValue());
			if (LRU.getTimeStamp().compareTo(swap.getTimeStamp()) < 0) // If LRU
																		// is
																		// less
																		// than
																		// swap
			{
				// LRU is LRU of these 2
			} else {
				// New LRU is the swap var
				LRU = new PageColumnMeta(swap);
				LRUindex = (int) mypair.getKey();
			}
		}
		return LRUindex;
	}

	// Gets the bucket and the offset for each phone substring matched
	public List<DataLocation> searchPage(List<DataLocation> datastrings,
			String phonequery) {
		// bucket, offset
		List<DataLocation> phonekeys = new ArrayList<DataLocation>();

		// iterate through each DataLocation in the page
		for (DataLocation dl : datastrings) {
			// if the string contains the area code, add its location to index
			// array
			if (dl.getDataString().substring(0, 3).contains(phonequery)) {
				DataLocation dummydata = new DataLocation();
				dummydata.setBucketNo(dl.getBucketNo());
				dummydata.setRowNo(dl.getRowNo());
				dummydata.setDataString(dl.getDataString());
				dummydata.setDataType(dl.getDataType());
				dummydata.setTableName(dl.getTableName());
				phonekeys.add(dummydata);
			}
		}
		return phonekeys;
	}

	public List<DataLocation> searchForName(List<DataLocation> nameLocs,
			List<DataLocation> answerLocs) {
		List<DataLocation> name_ans = new ArrayList<DataLocation>();

		for (DataLocation names : nameLocs) {
			for (DataLocation answers : answerLocs) {
				if (names.getBucketNo() == answers.getBucketNo()
						&& names.getRowNo() == answers.getRowNo()) {
					DataLocation dummyData = new DataLocation();
					dummyData.setBucketNo(answers.getBucketNo());
					dummyData.setDataString(names.getDataString());
					dummyData.setDataType("name");
					dummyData.setRowNo(answers.getRowNo());
					dummyData.setTableName(answers.getTableName());
					name_ans.add(dummyData);
				}
			}
		}

		return name_ans;
	}

	public List<DataLocation> searchForId(List<DataLocation> idLocs,
			List<DataLocation> answerLocs) {
		List<DataLocation> id_ans = new ArrayList<DataLocation>();

		for (DataLocation ids : idLocs) {
			for (DataLocation answers : answerLocs) {
				if (ids.getBucketNo() == answers.getBucketNo()
						&& ids.getRowNo() == answers.getRowNo()) {
					DataLocation dummyData = new DataLocation();
					dummyData.setBucketNo(answers.getBucketNo());
					dummyData.setDataString(ids.getDataString());
					dummyData.setDataType("id");
					dummyData.setRowNo(answers.getRowNo());
					dummyData.setTableName(answers.getTableName());
					id_ans.add(dummyData);
				}
			}
		}

		return id_ans;
	}

	public int countPage(List<DataLocation> datastrings, String phonequery) {
		int sumcount = 0;

		// iterate through each DataLocation in the page
		for (DataLocation dl : datastrings) {
			// if the string contains the area code, increment counter
			if (dl.getDataString().substring(0, 3).contains(phonequery)) {
				sumcount += 1;
			}
		}
		return sumcount;
	}

	public int getPage(DataLocation dl) {
		int pageNo = 0;
		int pageSize = 0;
		if (dl.getDataType().toLowerCase() == "id") {
			pageSize = 125;
		} else if (dl.getDataType().toLowerCase() == "name") {
			pageSize = 31;
		} else // phone
		{
			pageSize = 41; // check with Priyanka
		}

		// dataL.setRowNo(i + (126 * (dl.getRowNo()/126)));
		pageNo = dl.getRowNo() / pageSize;

		return pageNo;
	}

	public int getPageForDT(DataLocation dl, String datatype) {
		int pageNo = 0;
		int pageSize = 0;
		if (datatype.toLowerCase() == "id") {
			pageSize = 125;
		} else if (datatype.toLowerCase() == "name") {
			pageSize = 31;
		} else // phone
		{
			pageSize = 41; // check with Priyanka
		}

		pageNo = dl.getRowNo() / pageSize;

		return pageNo;
	}

	public void logWriter(String logentry) {
		try (PrintWriter out = new PrintWriter(new BufferedWriter(
				new FileWriter("D:\\DBTest\\logfile.txt", true)))) {
			out.println(logentry);
		} catch (IOException e) {

		}
	}
}