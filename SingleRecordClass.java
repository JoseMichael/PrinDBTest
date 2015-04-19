//this class contains the variables required for a single record i.e. Name, Tele and ID
public class SingleRecordClass {
String Name;
String ID;
String TelephoneNo;

public void setRecord(String id, String name, String tele)
{
	Name = name;
	TelephoneNo = tele;
	ID = id;
}

}
