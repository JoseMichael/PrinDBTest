import java.util.Iterator;
import java.util.List;

public class DiskPage {

	public String getValue(int recordNumber) {
		// String str = null;
		// System.out.println("Value being accessed is "+recordNumber);
		return values.get(recordNumber);
	}

	public int indexOf(String valueToCheck) {
		int i = 0;
		String check = getValue(i);
		while (valueToCheck != check) {
			i++;
			check = getValue(i);
		}
		return i;

	}

	// String value[];

	List<String> values;

	public void makeDiskPage(List<String> list) {
		values = list;
	}

	public int numberOfRecords() {
		return values.size();
	}

}
