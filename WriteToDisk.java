import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;


public class WriteToDisk {

	public List<Schema> newList= new ArrayList<Schema>();
	List<String> tableName=new ArrayList<String>();
	List<String> files=new ArrayList<String>();
	
	String id="-id.txt"; 
	String name="-name.txt"; 
	String phone="-phone.txt"; 
	
	String baseFileLoc="D:\\DBTest\\";
	String dataFile="D:\\DBTest\\X.txt";
	String metadataFile="D:\\DBTest\\metadata.txt";
	
	String base="200";
	String orOperator="|";
	String comma=",";
	String semiColon=";";
	int bucketCount=16;
	String dump=comma + "000" + semiColon;

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getPhone() {
		return phone;
	}

	public String getBase() {
		return base;
	}

	public String getOrOperator() {
		return orOperator;
	}

	public String getComma() {
		return comma;
	}

	public String getSemiColon() {
		return semiColon;
	}

	public int getBucketCount() {
		return bucketCount;
	}

	public String getDump() {
		return dump;
	}

	public List<Schema> getNewList() {
		return newList;
	}

	public void setNewList(List<Schema> newList) {
		this.newList = newList;
	}


	public List<Schema> readFile(String FileName)
	{
		List<Schema> list= new ArrayList<Schema>();
		try {
						
			BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(baseFileLoc + FileName),"Cp1252"));
			String line;
			String delim=",";
			int counter=0;
			StringTokenizer token; //Use to tokenize each line read from the file.
			
			try {
				while((line=br.readLine())!=null)
				{
					//Create a schema object
					Schema schemaObj=new Schema();
					//Tokenize the line read from the file.
					token=new StringTokenizer(line,delim);
					//Loop through the token and read each entity from that
					while(token.hasMoreElements())
					{
						//Since we have 3 different values in a line, 1st value is Id, 2nd is Name and 3rd is the Phone
						if(counter==0) //Assigns the id
						{							
							schemaObj.setId(Integer.parseInt(token.nextElement().toString()));
							counter++;
						}
						else
							if(counter==1) //assigns the name
							{
								schemaObj.setName(token.nextElement().toString());
								counter++;
							}
							else
								if(counter==2) //Assigns the Phone
								{
									schemaObj.setPhone(token.nextElement().toString());
									counter=0;
								}
					}
					
					//add the schema object to the list/.
					list.add(schemaObj);
					
					//System.out.println("Line:" + line);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return list;
	}
	
	public void formHeaders(String TableName) 
	{
		RandomAccessFile rmId;
		
		try 
		{
			for (int j = 0; j < 3; j++) 
			{
				if(j==0)
				{	
					rmId=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() +this.id, "rw");
				}
				else
					if(j==1)
						rmId=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.name, "rw");
					else
						rmId=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.phone, "rw");
				try 
				{				
					String start="001";
					rmId.write(start.getBytes());
					rmId.write(orOperator.getBytes());
					String string;
					for (int i = 1; i <=bucketCount; i++) 
					{					
						string=StringUtils.leftPad(Integer.toString(i), 2,"0");
						string=StringUtils.rightPad(string, string.length()+ dump.length(),dump);
						rmId.write(string.getBytes());	
					}
					rmId.close();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}				
		
	}
	
	public void insertData(String TableName) 
	{
		List<Schema> list=new ArrayList<Schema>();
		list= this.newList;
	
		int retVal=0;
		RandomAccessFile rm;
		int position;
		String slPageNumFromHeader="";
		byte[] slottedPageNumberHeaderBuff=null;
		for (int  counter= 1;  counter<= 3; counter++) 
		{
			try 
			{
				if(counter==1)
				{
					rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.id, "rw");
				}
				else
					if(counter==2)
						rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.name, "rw");
					else
						rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.phone, "rw");
				
				for (Schema iterator : list) 
				{
					Schema schema = (Schema)iterator;
					retVal=schema.getId()%bucketCount;
					if(retVal==0)
						retVal=16;
					
					position=7 * retVal;
					try 
					{
						rm.seek(position);												
						slottedPageNumberHeaderBuff=new byte[3];
						rm.read(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
						slPageNumFromHeader= new String(slottedPageNumberHeaderBuff);
						System.out.println("slPageNumFromHeader:" + slPageNumFromHeader);
						if(slPageNumFromHeader.equals("000"))
						{
							rm.seek(0);
							slottedPageNumberHeaderBuff=new byte[3];
							rm.read(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
							String firstEmptySlot=new String(slottedPageNumberHeaderBuff);
							int currNumber=Integer.parseInt(firstEmptySlot);
							rm.seek(0);
							String writeBack=StringUtils.leftPad(Integer.toString(currNumber+1), 3,"0");
							slottedPageNumberHeaderBuff=(writeBack).getBytes();
							rm.write(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
							rm.seek(position);
							slottedPageNumberHeaderBuff=(StringUtils.leftPad(Integer.toString(currNumber), 3,"0")).getBytes();
							rm.write(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
							int pageStartAddress=calculatePositionOfSlottedPage(currNumber);
							System.out.println("pageStartAddress" + Integer.toString(pageStartAddress));
							
							//Code for storing Id
							if(counter==1)
							{
								//Check if the page is completely empty
								position=pageStartAddress-1;
								rm.seek(position);
								byte[] idBuff=new byte[4];
								rm.read(idBuff, 0, idBuff.length);
								String idVal=new String(idBuff);
								System.out.println("Length of Id Buff:" + idVal.length());
								if(idVal.trim().equals(""))
								{
									idVal=StringUtils.leftPad(Integer.toString(schema.getId()), 4);
									idBuff=idVal.getBytes();
									rm.seek(position);
									rm.write(idBuff,0, idBuff.length);	
									int lastBytePosition=position +4;
									System.out.println("\nLast Byte:" + lastBytePosition);
									int lastPointerPosition=pageStartAddress+ 509;
									position=lastPointerPosition-1;
									rm.seek(position);
									int valToBeStored=lastBytePosition-pageStartAddress+1;
									byte[] addBuff=new byte[3];
									addBuff=StringUtils.leftPad(Integer.toString(valToBeStored),3).getBytes();
									rm.write(addBuff, 0, addBuff.length);
									
									
								}
								else
								{
									//Slotted page is not completely empty.
									System.out.println("\nNot Empty for if"+ idVal +".");
									//Get the position of byte from 507-509
									int pointerLocationByteStart=position+ 507;
									rm.seek(pointerLocationByteStart-1);
									byte[] pointerBuff=new byte[3];
									rm.read(pointerBuff, 0, pointerBuff.length);
									String nextPageNumber= new String(pointerBuff);
									if(nextPageNumber.trim().equals(""))
									{
										System.out.println("\nEmpty for else");
									}
									else
									{
										System.out.println("\n Not empty for else");
									}
									
								}																	
								
							}
							else
								if(counter==2)
								{
									//Check if the page is completely empty
									position=pageStartAddress-1;
									rm.seek(position);
									byte[] nameBuff=new byte[16];
									rm.read(nameBuff, 0, nameBuff.length);
									String nameVal=new String(nameBuff);
									//System.out.println("Length of Id Buff:" + idVal.length());
									if(nameVal.trim().equals(""))
									{
										nameVal=StringUtils.leftPad(schema.getName(), 16);
										nameBuff=nameVal.getBytes();
										rm.seek(position);
										rm.write(nameBuff,0, nameBuff.length);	
										int lastBytePosition=position +16;
										System.out.println("\nLast Byte:" + lastBytePosition);
										int lastPointerPosition=pageStartAddress+ 509;
										position=lastPointerPosition-1;
										rm.seek(position);
										int valToBeStored=lastBytePosition-pageStartAddress+1;
										byte[] addBuff=new byte[3];
										addBuff=StringUtils.leftPad(Integer.toString(valToBeStored),3).getBytes();
										rm.write(addBuff, 0, addBuff.length);																			
									}
									else
									{
										//Slotted page is not completely empty.
										//System.out.println("\nNot Empty for if"+ nameVal +".");
										//Get the position of byte from 507-509
										int pointerLocationByteStart=position+ 507;
										rm.seek(pointerLocationByteStart-1);
										byte[] pointerBuff=new byte[3];
										rm.read(pointerBuff, 0, pointerBuff.length);
										String nextPageNumber= new String(pointerBuff);
										if(nextPageNumber.trim().equals(""))
										{
											System.out.println("\nEmpty for else");
										}
										else
										{
											System.out.println("\n Not empty for else");
										}
										
									}										
								}
								else
									if(counter==3)
									{
										//Check if the page is completely empty
										position=pageStartAddress-1;
										rm.seek(position);
										byte[] phoneBuff=new byte[12];
										rm.read(phoneBuff, 0, phoneBuff.length);
										String phoneVal=new String(phoneBuff);
										System.out.println("Length of Id Buff:" + phoneVal.length());
										if(phoneVal.trim().equals(""))
										{
											phoneVal=StringUtils.leftPad(schema.getPhone(), 12);
											phoneBuff=phoneVal.getBytes();
											rm.seek(position);
											rm.write(phoneBuff,0, phoneBuff.length);	
											int lastBytePosition=position +12;
											System.out.println("\nLast Byte:" + lastBytePosition);
											int lastPointerPosition=pageStartAddress+ 509;
											position=lastPointerPosition-1;
											rm.seek(position);
											int valToBeStored=lastBytePosition-pageStartAddress+1;
											byte[] addBuff=new byte[3];
											addBuff=StringUtils.leftPad(Integer.toString(valToBeStored),3).getBytes();
											rm.write(addBuff, 0, addBuff.length);
											
										}
										else
										{
											//Slotted page is not completely empty.
											System.out.println("\nNot Empty for if"+ phoneVal +".");
											//Get the position of byte from 507-509
											int pointerLocationByteStart=position+ 507;
											rm.seek(pointerLocationByteStart-1);
											byte[] pointerBuff=new byte[3];
											rm.read(pointerBuff, 0, pointerBuff.length);
											String nextPageNumber= new String(pointerBuff);
											if(nextPageNumber.trim().equals(""))
											{
												System.out.println("\nEmpty for else");
											}
											else
											{
												System.out.println("\n Not empty for else");
											}
											
										}	
										/*rm.seek(position);
										idBuff=new byte[4];
										rm.read(idBuff, 0, idBuff.length);
										idVal=new String(idBuff);
										System.out.println("Buff:" + idVal);*/

									}
						}
						else
						{
							int pageStartAddress=calculatePositionOfSlottedPage(Integer.parseInt(slPageNumFromHeader.trim()));
							System.out.println("pageStartAddress" + Integer.toString(pageStartAddress));
							int lastPointerPositionAddress=0;
							//Code for storing Id
							if(counter==1)
							{
								//Check if the page is completely empty
								position=pageStartAddress-1;
								rm.seek(position);
								byte[] idBuff=new byte[4];
								rm.read(idBuff, 0, idBuff.length);
								String idVal=new String(idBuff);
								System.out.println("Length of Id Buff:" + idVal.length());
								if(idVal.trim().equals(""))
								{
									idVal=StringUtils.leftPad(Integer.toString(schema.getId()), 4);
									idBuff=idVal.getBytes();
									rm.seek(position);
									rm.write(idBuff,0, idBuff.length);	
									int lastBytePosition=position +4;
									System.out.println("\nLast Byte:" + lastBytePosition);
									lastPointerPositionAddress=pageStartAddress+ 509;
									position=lastPointerPositionAddress-1;
									rm.seek(position);
									int valToBeStored=lastBytePosition-pageStartAddress+1;
									byte[] addBuff=new byte[3];
									addBuff=StringUtils.leftPad(Integer.toString(valToBeStored),3).getBytes();
									rm.write(addBuff, 0, addBuff.length);
									/*rm.seek(lastPointerPosition-1);
									idBuff=new byte[3];
									rm.read(idBuff, 0, idBuff.length);
									idVal=new String(idBuff);
									System.out.println("Last Pointer value:" + idVal);*/
									
								}
								else
								{
									//Slotted page is not completely empty.
									System.out.println("\nNot Empty for if"+ idVal +".");
									//Get the position of byte from 507-509
									position=pageStartAddress-1;  //*****New 
									int pointerLocationByteStart=position + 507;
									rm.seek(pointerLocationByteStart-1);
									byte[] pointerBuff=new byte[3];
									rm.read(pointerBuff, 0, pointerBuff.length);
									String nextPageNumber= new String(pointerBuff);
									if(nextPageNumber.trim().equals("")) //check if this page is full and has a pointer to another slotted page
									{
										System.out.println("\nPage is not full.");
																				
										lastPointerPositionAddress=pageStartAddress+ 509;
										position=lastPointerPositionAddress-1;
										rm.seek(position);
										//Collect the value of the last allocated byte
										byte[] lastPointerValueBuff=new byte[3];
										rm.read(lastPointerValueBuff, 0, lastPointerValueBuff.length);
										
										String lastPointerValue=new String(lastPointerValueBuff);
										//System.out.println("Length of Id Buff:" + lastPointerValue.length());
										int lastPointer= Integer.parseInt(lastPointerValue.trim());
										
										//Check if there is enough space to store the current data
										int lastByteAddressForWriting=lastPointerPositionAddress-4;
										int lastWrittenByteAddress=pageStartAddress -1 + lastPointer;
										if(lastByteAddressForWriting-lastWrittenByteAddress>=4)
										{
											position=pageStartAddress-1 + lastPointer;
											rm.seek(position);
											idBuff=new byte[4];
											idVal=StringUtils.leftPad(Integer.toString(schema.getId()), 4);
											idBuff=idVal.getBytes();
											rm.write(idBuff,0, idBuff.length);
											//write the last byte of this at the last 3 bytes of this page
											lastPointer=lastPointer+4; //increment the last pointer by 4
											position=lastPointerPositionAddress-1;
											rm.seek(position);
											byte[] addBuff=new byte[3];
											addBuff=StringUtils.leftPad(Integer.toString(lastPointer),3).getBytes();
											rm.write(addBuff, 0, addBuff.length);											
										}
										else //Allocate a new slotted page for this data
										{
											rm.seek(0);
											slottedPageNumberHeaderBuff=new byte[3];
											rm.read(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
											String firstEmptySlot=new String(slottedPageNumberHeaderBuff);
											int currNumber=Integer.parseInt(firstEmptySlot);
											rm.seek(0);
											//Update the firstAvailable block count
											String writeBack=StringUtils.leftPad(Integer.toString(currNumber+1), 3,"0");
											slottedPageNumberHeaderBuff=(writeBack).getBytes();
											rm.write(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
											
											//Store the new slotted page number in the next page pointer field of the current block.
											//Get the position of byte from 507-509
											pointerLocationByteStart=pageStartAddress -1 + 507;
											rm.seek(pointerLocationByteStart-1);
											pointerBuff=new byte[3];
											pointerBuff=StringUtils.leftPad(Integer.toString(currNumber),3).getBytes() ;
											rm.write(pointerBuff, 0, pointerBuff.length);
																						
											int getPageAddress=calculatePositionOfSlottedPage(currNumber);
											//Move to the next page address-1 location
											position=getPageAddress-1; 
											rm.seek(position);
											idBuff=new byte[4];
											idBuff= StringUtils.leftPad(Integer.toString(schema.getId()),4).getBytes();
											rm.write(idBuff, 0, idBuff.length);
											//Write the last byte count to the end of this block.
											lastPointerPositionAddress=getPageAddress+ 509; ///*****
											position=lastPointerPositionAddress-1;
											rm.seek(position);
											byte[] addBuff=new byte[3];
											addBuff=StringUtils.leftPad(Integer.toString(4),3).getBytes();
											rm.write(addBuff, 0, addBuff.length);																																	
										}

									}
									else//this slotted page is full
									{
										System.out.println("\n Page is full.");
										//*****
										int existingNextPageNumber=0;
										int getPageAddress=0;
										byte[] addBuff;
										String addVal;
										while(true)
										{												
											existingNextPageNumber=Integer.parseInt(nextPageNumber.trim());
											getPageAddress=calculatePositionOfSlottedPage(existingNextPageNumber);												
											position=getPageAddress-1 + 506; //this takes you a location before the next address field of the new page
											rm.seek(position);
											addBuff=new byte[3];
											rm.read(addBuff, 0, addBuff.length);
											addVal= new String(addBuff);
											if(addVal.trim().length()==0) //If this page is not full and has a link to another slotted page.
											{
												break;
											}
											else
												nextPageNumber=addVal;																																		
										}
										
										//Move to its last 3 bytes and get the pointer location for the last occupied byte of this block
										position=getPageAddress-1 + 509;
										rm.seek(position);
										addBuff=new byte[3];
										rm.read(addBuff, 0, addBuff.length);
										addVal= new String(addBuff);
										int lastOccupiedByteVal=Integer.parseInt(addVal.trim());
										int lastOccupiedByteLocation=getPageAddress-1 +lastOccupiedByteVal ; //Still here
										int lastByteAddressForWriting=getPageAddress + 505;
										if(lastByteAddressForWriting-lastOccupiedByteLocation>=4)// space available for writing data
										{
											//we can write data here
											position=lastOccupiedByteLocation;
											rm.seek(position);
											idBuff=new byte[4];
											idBuff= StringUtils.leftPad(Integer.toString(schema.getId()),4).getBytes();
											rm.write(idBuff, 0, idBuff.length);
											//Write the last byte count to the end of this block.
											lastPointerPositionAddress=getPageAddress+ 509;
											position=lastPointerPositionAddress-1;
											rm.seek(position);
											addBuff=new byte[3];
											lastOccupiedByteVal=lastOccupiedByteVal+4; ////Newwww
											addBuff=StringUtils.leftPad(Integer.toString(lastOccupiedByteVal),3).getBytes(); ////////Here you go											
											rm.write(addBuff, 0, addBuff.length);																																											
										}
										else //Not enough space for writing data
										{
											rm.seek(0);
											slottedPageNumberHeaderBuff=new byte[3];
											rm.read(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
											String firstEmptySlot=new String(slottedPageNumberHeaderBuff);
											int currNumber=Integer.parseInt(firstEmptySlot);
											rm.seek(0);
											//Update the firstAvailable block count
											String writeBack=StringUtils.leftPad(Integer.toString(currNumber+1), 3,"0");
											slottedPageNumberHeaderBuff=(writeBack).getBytes();
											rm.write(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
											
											//Store the new slotted page number in the next page pointer field of the current block.
											//Get the position of byte from 507-509
											pointerLocationByteStart=getPageAddress -1 + 507;
											rm.seek(pointerLocationByteStart-1);
											pointerBuff=new byte[3];
											pointerBuff=StringUtils.leftPad(Integer.toString(currNumber),3).getBytes() ;
											rm.write(pointerBuff, 0, pointerBuff.length);
																						
											getPageAddress=calculatePositionOfSlottedPage(currNumber);
											//Move to the next page address-1 location
											position=getPageAddress-1;
											rm.seek(position);
											
											idBuff=new byte[4];
											idBuff= StringUtils.leftPad(Integer.toString(schema.getId()),4).getBytes();
											rm.write(idBuff, 0, idBuff.length);
											//Write the last byte count to the end of this block.
											lastPointerPositionAddress=getPageAddress+ 509;
											position=lastPointerPositionAddress-1;
											rm.seek(position);
											addBuff=new byte[3];
											addBuff=StringUtils.leftPad(Integer.toString(4),3).getBytes();
											rm.write(addBuff, 0, addBuff.length);	
											
										}																																																				
									}
									
								}	//till here
								/*rm.seek(position);
								idBuff=new byte[4];
								rm.read(idBuff, 0, idBuff.length);
								idVal=new String(idBuff);
								System.out.println("Buff:" + idVal);*/
								
								
							}
							else
								if(counter==2)
								{
									//Check if the page is completely empty
									position=pageStartAddress-1;
									rm.seek(position);
									byte[] nameBuff=new byte[16];
									rm.read(nameBuff, 0, nameBuff.length);
									String nameVal=new String(nameBuff);
									System.out.println("Length of Id Buff:" + nameVal.length());
									if(nameVal.trim().equals(""))
									{
										nameVal=StringUtils.leftPad(schema.getName(), 16);
										nameBuff=nameVal.getBytes();
										rm.seek(position);
										rm.write(nameBuff,0, nameBuff.length);	
										int lastBytePosition=position +16;
										System.out.println("\nLast Byte:" + lastBytePosition);
										lastPointerPositionAddress=pageStartAddress+ 509;
										position=lastPointerPositionAddress-1;
										rm.seek(position);
										int valToBeStored=lastBytePosition-pageStartAddress+1;
										byte[] addBuff=new byte[3];
										addBuff=StringUtils.leftPad(Integer.toString(valToBeStored),3).getBytes();
										rm.write(addBuff, 0, addBuff.length);
										/*rm.seek(lastPointerPosition-1);
										idBuff=new byte[3];
										rm.read(idBuff, 0, idBuff.length);
										idVal=new String(idBuff);
										System.out.println("Last Pointer value:" + idVal);*/
										
									}
									else
									{
										//Slotted page is not completely empty.
										System.out.println("\nNot Empty for if"+ nameVal +".");
										//Get the position of byte from 507-509
										position=pageStartAddress-1;  //*****New 
										int pointerLocationByteStart=position + 507;
										rm.seek(pointerLocationByteStart-1);
										byte[] pointerBuff=new byte[3];
										rm.read(pointerBuff, 0, pointerBuff.length);
										String nextPageNumber= new String(pointerBuff);
										if(nextPageNumber.trim().equals("")) //check if this page is full and has a pointer to another slotted page
										{
											System.out.println("\nPage is not full.");
																					
											lastPointerPositionAddress=pageStartAddress+ 509;
											position=lastPointerPositionAddress-1;
											rm.seek(position);
											//Collect the value of the last allocated byte
											byte[] lastPointerValueBuff=new byte[3];
											rm.read(lastPointerValueBuff, 0, lastPointerValueBuff.length);
											
											String lastPointerValue=new String(lastPointerValueBuff);
											//System.out.println("Length of Id Buff:" + lastPointerValue.length());
											int lastPointer= Integer.parseInt(lastPointerValue.trim());
											
											//Check if there is enough space to store the current data
											int lastByteAddressForWriting=lastPointerPositionAddress-4;
											int lastWrittenByteAddress=pageStartAddress -1 + lastPointer;
											if(lastByteAddressForWriting-lastWrittenByteAddress>=16)
											{
												position=pageStartAddress-1 + lastPointer;
												rm.seek(position);
												nameBuff=new byte[16];
												nameVal=StringUtils.leftPad(schema.getName(), 16);
												nameBuff=nameVal.getBytes();
												rm.write(nameBuff,0, nameBuff.length);
												//write the last byte of this at the last 3 bytes of this page
												lastPointer=lastPointer+16; //increment the last pointer by 4
												position=lastPointerPositionAddress-1;
												rm.seek(position);
												byte[] addBuff=new byte[3];
												addBuff=StringUtils.leftPad(Integer.toString(lastPointer),3).getBytes();
												rm.write(addBuff, 0, addBuff.length);											
											}
											else //Allocate a new slotted page for this data
											{
												rm.seek(0);
												slottedPageNumberHeaderBuff=new byte[3];
												rm.read(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
												String firstEmptySlot=new String(slottedPageNumberHeaderBuff);
												int currNumber=Integer.parseInt(firstEmptySlot);
												rm.seek(0);
												//Update the firstAvailable block count
												String writeBack=StringUtils.leftPad(Integer.toString(currNumber+1), 3,"0");
												slottedPageNumberHeaderBuff=(writeBack).getBytes();
												rm.write(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
												
												//Store the new slotted page number in the next page pointer field of the current block.
												//Get the position of byte from 507-509
												pointerLocationByteStart=pageStartAddress -1 + 507;
												rm.seek(pointerLocationByteStart-1);
												pointerBuff=new byte[3];
												pointerBuff=StringUtils.leftPad(Integer.toString(currNumber),3).getBytes() ;
												rm.write(pointerBuff, 0, pointerBuff.length);
																							
												int getPageAddress=calculatePositionOfSlottedPage(currNumber);
												//Move to the next page address-1 location
												position=getPageAddress-1; 
												rm.seek(position);
												nameBuff=new byte[16];
												nameBuff= StringUtils.leftPad(schema.getName(),16).getBytes();
												rm.write(nameBuff, 0, nameBuff.length);
												//Write the last byte count to the end of this block.
												lastPointerPositionAddress=getPageAddress+ 509; ///*****
												position=lastPointerPositionAddress-1;
												rm.seek(position);
												byte[] addBuff=new byte[3];
												addBuff=StringUtils.leftPad(Integer.toString(16),3).getBytes();
												rm.write(addBuff, 0, addBuff.length);																																	
											}

										}
										else//this slotted page is full
										{
											System.out.println("\n Page is full.");
											//*****
											int existingNextPageNumber=0;
											int getPageAddress=0;
											byte[] addBuff;
											String addVal;
											while(true)
											{												
												existingNextPageNumber=Integer.parseInt(nextPageNumber.trim());
												getPageAddress=calculatePositionOfSlottedPage(existingNextPageNumber);												
												position=getPageAddress-1 + 506; //this takes you a location before the next address field of the new page
												rm.seek(position);
												addBuff=new byte[3];
												rm.read(addBuff, 0, addBuff.length);
												addVal= new String(addBuff);
												if(addVal.trim().length()==0) //If this page is not full and has a link to another slotted page.
												{
													break;
												}
												else
													nextPageNumber=addVal;																																		
											}
											
											//Move to its last 3 bytes and get the pointer location for the last occupied byte of this block
											position=getPageAddress-1 + 509;
											rm.seek(position);
											addBuff=new byte[3];
											rm.read(addBuff, 0, addBuff.length);
											addVal= new String(addBuff);
											int lastOccupiedByteVal=Integer.parseInt(addVal.trim());
											int lastOccupiedByteLocation=getPageAddress-1 +lastOccupiedByteVal ; //Still here
											int lastByteAddressForWriting=getPageAddress + 505;
											if(lastByteAddressForWriting-lastOccupiedByteLocation>=16)// space available for writing data
											{
												//we can write data here
												position=lastOccupiedByteLocation;
												rm.seek(position);
												nameBuff=new byte[16];
												nameBuff= StringUtils.leftPad(schema.getName(),16).getBytes();
												rm.write(nameBuff, 0, nameBuff.length);
												//Write the last byte count to the end of this block.
												lastPointerPositionAddress=getPageAddress+ 509;
												position=lastPointerPositionAddress-1;
												rm.seek(position);
												addBuff=new byte[3];
												lastOccupiedByteVal=lastOccupiedByteVal+16; ////Newwww
												addBuff=StringUtils.leftPad(Integer.toString(lastOccupiedByteVal),3).getBytes(); ////////Here you go
												rm.write(addBuff, 0, addBuff.length);																																											
											}
											else //Not enough space for writing data
											{
												rm.seek(0);
												slottedPageNumberHeaderBuff=new byte[3];
												rm.read(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
												String firstEmptySlot=new String(slottedPageNumberHeaderBuff);
												int currNumber=Integer.parseInt(firstEmptySlot);
												rm.seek(0);
												//Update the firstAvailable block count
												String writeBack=StringUtils.leftPad(Integer.toString(currNumber+1), 3,"0");
												slottedPageNumberHeaderBuff=(writeBack).getBytes();
												rm.write(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
												
												//Store the new slotted page number in the next page pointer field of the current block.
												//Get the position of byte from 507-509
												pointerLocationByteStart=getPageAddress -1 + 507;
												rm.seek(pointerLocationByteStart-1);
												pointerBuff=new byte[3];
												pointerBuff=StringUtils.leftPad(Integer.toString(currNumber),3).getBytes() ;
												rm.write(pointerBuff, 0, pointerBuff.length);
																							
												getPageAddress=calculatePositionOfSlottedPage(currNumber);
												//Move to the next page address-1 location
												position=getPageAddress-1;
												rm.seek(position);
												
												nameBuff=new byte[16];
												nameBuff= StringUtils.leftPad(schema.getName(),16).getBytes();
												rm.write(nameBuff, 0, nameBuff.length);
												//Write the last byte count to the end of this block.
												lastPointerPositionAddress=getPageAddress+ 509;
												position=lastPointerPositionAddress-1;
												rm.seek(position);
												addBuff=new byte[3];
												addBuff=StringUtils.leftPad(Integer.toString(16),3).getBytes();
												rm.write(addBuff, 0, addBuff.length);	
												
											}																																																				
										}
										
									}	//tille 
									/*rm.seek(position);
									idBuff=new byte[4];
									rm.read(idBuff, 0, idBuff.length);
									idVal=new String(idBuff);
									System.out.println("Buff:" + idVal);*/
																		
								}
								else
									if(counter==3)
									{
										//Check if the page is completely empty
										position=pageStartAddress-1;
										rm.seek(position);
										byte[] phoneBuff=new byte[12];
										rm.read(phoneBuff, 0, phoneBuff.length);
										String phoneVal=new String(phoneBuff);
										System.out.println("Length of Id Buff:" + phoneVal.length());
										if(phoneVal.trim().equals(""))
										{
											phoneVal=StringUtils.leftPad(schema.getPhone(), 12);
											phoneBuff=phoneVal.getBytes();
											rm.seek(position);
											rm.write(phoneBuff,0, phoneBuff.length);	
											int lastBytePosition=position +12;
											System.out.println("\nLast Byte:" + lastBytePosition);
											lastPointerPositionAddress=pageStartAddress+ 509;
											position=lastPointerPositionAddress-1;
											rm.seek(position);
											int valToBeStored=lastBytePosition-pageStartAddress+1;
											byte[] addBuff=new byte[3];
											addBuff=StringUtils.leftPad(Integer.toString(valToBeStored),3).getBytes();
											rm.write(addBuff, 0, addBuff.length);
											/*rm.seek(lastPointerPosition-1);
											idBuff=new byte[3];
											rm.read(idBuff, 0, idBuff.length);
											idVal=new String(idBuff);
											System.out.println("Last Pointer value:" + idVal);*/
											
										}
										else
										{
											//Slotted page is not completely empty.
											System.out.println("\nNot Empty for if"+ phoneVal +".");
											//Get the position of byte from 507-509
											position=pageStartAddress-1;  //*****New 
											int pointerLocationByteStart=position + 507;
											rm.seek(pointerLocationByteStart-1);
											byte[] pointerBuff=new byte[3];
											rm.read(pointerBuff, 0, pointerBuff.length);
											String nextPageNumber= new String(pointerBuff);
											if(nextPageNumber.trim().equals("")) //check if this page is full and has a pointer to another slotted page
											{
												System.out.println("\nPage is not full.");
																						
												lastPointerPositionAddress=pageStartAddress+ 509;
												position=lastPointerPositionAddress-1;
												rm.seek(position);
												//Collect the value of the last allocated byte
												byte[] lastPointerValueBuff=new byte[3];
												rm.read(lastPointerValueBuff, 0, lastPointerValueBuff.length);
												
												String lastPointerValue=new String(lastPointerValueBuff);
												//System.out.println("Length of Id Buff:" + lastPointerValue.length());
												int lastPointer= Integer.parseInt(lastPointerValue.trim());
												
												//Check if there is enough space to store the current data
												int lastByteAddressForWriting=lastPointerPositionAddress-4;
												int lastWrittenByteAddress=pageStartAddress -1 + lastPointer;
												if(lastByteAddressForWriting-lastWrittenByteAddress>=12)
												{
													position=pageStartAddress-1 + lastPointer;
													rm.seek(position);
													phoneBuff=new byte[12];
													phoneVal=StringUtils.leftPad(schema.getPhone(), 12);
													phoneBuff=phoneVal.getBytes();
													rm.write(phoneBuff,0, phoneBuff.length);
													//write the last byte of this at the last 3 bytes of this page
													lastPointer=lastPointer+12; //increment the last pointer by 4
													position=lastPointerPositionAddress-1;
													rm.seek(position);
													byte[] addBuff=new byte[3];
													addBuff=StringUtils.leftPad(Integer.toString(lastPointer),3).getBytes();
													rm.write(addBuff, 0, addBuff.length);											
												}
												else //Allocate a new slotted page for this data
												{
													rm.seek(0);
													slottedPageNumberHeaderBuff=new byte[3];
													rm.read(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
													String firstEmptySlot=new String(slottedPageNumberHeaderBuff);
													int currNumber=Integer.parseInt(firstEmptySlot);
													rm.seek(0);
													//Update the firstAvailable block count
													String writeBack=StringUtils.leftPad(Integer.toString(currNumber+1), 3,"0");
													slottedPageNumberHeaderBuff=(writeBack).getBytes();
													rm.write(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
													
													//Store the new slotted page number in the next page pointer field of the current block.
													//Get the position of byte from 507-509
													pointerLocationByteStart=pageStartAddress -1 + 507;
													rm.seek(pointerLocationByteStart-1);
													pointerBuff=new byte[3];
													pointerBuff=StringUtils.leftPad(Integer.toString(currNumber),3).getBytes() ;
													rm.write(pointerBuff, 0, pointerBuff.length);
																								
													int getPageAddress=calculatePositionOfSlottedPage(currNumber);
													//Move to the next page address-1 location
													position=getPageAddress-1; 
													rm.seek(position);
													phoneBuff=new byte[12];
													phoneBuff= StringUtils.leftPad(schema.getPhone(),12).getBytes();
													rm.write(phoneBuff, 0, phoneBuff.length);
													//Write the last byte count to the end of this block.
													lastPointerPositionAddress=getPageAddress+ 509; ///*****
													position=lastPointerPositionAddress-1;
													rm.seek(position);
													byte[] addBuff=new byte[3];
													addBuff=StringUtils.leftPad(Integer.toString(12),3).getBytes();
													rm.write(addBuff, 0, addBuff.length);																																	
												}

											}
											else//this slotted page is full
											{
												System.out.println("\n Page is full.");
												//*****
												int existingNextPageNumber=0;
												int getPageAddress=0;
												byte[] addBuff;
												String addVal;
												while(true)
												{												
													existingNextPageNumber=Integer.parseInt(nextPageNumber.trim());
													getPageAddress=calculatePositionOfSlottedPage(existingNextPageNumber);												
													position=getPageAddress-1 + 506; //this takes you a location before the next address field of the new page
													rm.seek(position);
													addBuff=new byte[3];
													rm.read(addBuff, 0, addBuff.length);
													addVal= new String(addBuff);
													if(addVal.trim().length()==0) //If this page is not full and has a link to another slotted page.
													{
														break;
													}
													else
														nextPageNumber=addVal;																																		
												}
												
												//Move to its last 3 bytes and get the pointer location for the last occupied byte of this block
												position=getPageAddress-1 + 509;
												rm.seek(position);
												addBuff=new byte[3];
												rm.read(addBuff, 0, addBuff.length);
												addVal= new String(addBuff);
												int lastOccupiedByteVal=Integer.parseInt(addVal.trim());
												int lastOccupiedByteLocation=getPageAddress-1 +lastOccupiedByteVal ; //Still here
												int lastByteAddressForWriting=getPageAddress + 505;
												if(lastByteAddressForWriting-lastOccupiedByteLocation>=12)// space available for writing data
												{
													//we can write data here
													position=lastOccupiedByteLocation;
													rm.seek(position);
													phoneBuff=new byte[12];
													phoneBuff= StringUtils.leftPad(schema.getPhone(),12).getBytes();
													rm.write(phoneBuff, 0, phoneBuff.length);
													//Write the last byte count to the end of this block.
													lastPointerPositionAddress=getPageAddress+ 509;
													position=lastPointerPositionAddress-1;
													rm.seek(position);
													addBuff=new byte[3];
													lastOccupiedByteVal=lastOccupiedByteVal+12; ////Newwww
													addBuff=StringUtils.leftPad(Integer.toString(lastOccupiedByteVal),3).getBytes(); ////////Here you go
													rm.write(addBuff, 0, addBuff.length);																																											
												}
												else //Not enough space for writing data
												{
													rm.seek(0);
													slottedPageNumberHeaderBuff=new byte[3];
													rm.read(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
													String firstEmptySlot=new String(slottedPageNumberHeaderBuff);
													int currNumber=Integer.parseInt(firstEmptySlot);
													rm.seek(0);
													//Update the firstAvailable block count
													String writeBack=StringUtils.leftPad(Integer.toString(currNumber+1), 3,"0");
													slottedPageNumberHeaderBuff=(writeBack).getBytes();
													rm.write(slottedPageNumberHeaderBuff, 0, slottedPageNumberHeaderBuff.length);
													
													//Store the new slotted page number in the next page pointer field of the current block.
													//Get the position of byte from 507-509
													pointerLocationByteStart=getPageAddress -1 + 507;
													rm.seek(pointerLocationByteStart-1);
													pointerBuff=new byte[3];
													pointerBuff=StringUtils.leftPad(Integer.toString(currNumber),3).getBytes() ;
													rm.write(pointerBuff, 0, pointerBuff.length);
																								
													getPageAddress=calculatePositionOfSlottedPage(currNumber);
													//Move to the next page address-1 location
													position=getPageAddress-1;
													rm.seek(position);
													
													phoneBuff=new byte[12];
													phoneBuff= StringUtils.leftPad(schema.getPhone(),12).getBytes();
													rm.write(phoneBuff, 0, phoneBuff.length);
													//Write the last byte count to the end of this block.
													lastPointerPositionAddress=getPageAddress+ 509;
													position=lastPointerPositionAddress-1;
													rm.seek(position);
													addBuff=new byte[3];
													addBuff=StringUtils.leftPad(Integer.toString(12),3).getBytes();
													rm.write(addBuff, 0, addBuff.length);	
													
												}																																																				
											}
											
										}	//till here
										/*rm.seek(position);
										idBuff=new byte[4];
										rm.read(idBuff, 0, idBuff.length);
										idVal=new String(idBuff);
										System.out.println("Buff:" + idVal);*/
										
									}
						}
						
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
				try {
					rm.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
		File meta=new File(metadataFile);
		if(meta.exists())
		{
			//meta.delete();
			
		}
		try 
		{
			meta.createNewFile();
			FileWriter fw = new FileWriter(meta.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			for (String table: tableName) 
			{
				
				bw.write(table + this.id );
				bw.newLine();
				bw.write(table + this.name );
				bw.newLine();
				bw.write(table + this.phone);
				bw.newLine();
			}
			bw.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public int calculatePositionOfSlottedPage(int slottedPageNumber) 
	{
		int pageStartsFrom=0;
		int baseInt=Integer.parseInt(base);
		
		pageStartsFrom= baseInt + (slottedPageNumber-1) * 512 +1 ;
		
		return pageStartsFrom;
	}
	
	
	@SuppressWarnings("resource")
	public List<String> getDiskPage(String TableName, String AttributeName, int BucketNumber)
	{
		List<ReadDiskPageAddress> list=new ArrayList<ReadDiskPageAddress>();	
		List<String> returnList=new ArrayList<String>();	
		RandomAccessFile rm;	
		int position=0;
		String val;
		position=7 * BucketNumber;
		String pageNo, lastByteLoc;
		int pageNumAddress,pageNum,bytecount;
		byte[] addBuff;
		try 
		{
			if(tableName.contains(TableName.toUpperCase()))
			{
				if(AttributeName.toLowerCase().equals("id"))
				{
					rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.id, "rw");
					try 
					{
						byte[] idBuff;
						String idVal;
						//position= Integer.parseInt(base);
						while(true)
						{
							rm.seek(position);	
							byte[] pageNumber=new byte[3];
							rm.read(pageNumber, 0, pageNumber.length);
							val=new String(pageNumber);
							pageNo=val.trim();
						    if(pageNo.length()>0 && (!"000".equals(pageNo)))
						    {
						    	pageNumAddress=calculatePositionOfSlottedPage(Integer.parseInt(pageNo));
						    	position=pageNumAddress+508;
						    	rm.seek(position);
						    	addBuff=new byte[3];
						    	rm.read(addBuff, 0, addBuff.length);
						    	lastByteLoc=new String(addBuff);
						    	if(lastByteLoc.trim().length()>0)
						    	{
						    		ReadDiskPageAddress rd=new ReadDiskPageAddress();
						    		rd.setPageNumber(pageNo);
						    		rd.setLastBytePointerLocation(lastByteLoc.trim());
						    		list.add(rd);
						    		position=pageNumAddress + 505;
						    	}
						    }
						    else
						    	break;
						}
						
					    for (ReadDiskPageAddress rd : list)
					    {
							ReadDiskPageAddress readDiskPageAddress = rd;
							pageNum= Integer.parseInt(rd.getPageNumber().trim());
							pageNumAddress=calculatePositionOfSlottedPage(pageNum);
							bytecount=Integer.parseInt(rd.getLastBytePointerLocation().trim());
							int myCount=bytecount/4;
							position=pageNumAddress-1;
							for(int i=0;i<myCount;i++)
							{			
								position= pageNumAddress-1 + 4 *i;
								rm.seek(position);
								idBuff=new byte[4];
								rm.read(idBuff, 0, idBuff.length);
								idVal=new String(idBuff);
								returnList.add(idVal.trim());
							}
						}
					    rm.close();
						
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			else
				if(AttributeName.toLowerCase().equals("name"))
				{
					rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.name, "rw");
					
					try 
					{
						byte[] nameBuff;
						String nameVal;
						//position= Integer.parseInt(base);
						while(true)
						{
							rm.seek(position);	
							byte[] pageNumber=new byte[3];
							rm.read(pageNumber, 0, pageNumber.length);
							val=new String(pageNumber);
							pageNo=val.trim();
						    if(pageNo.length()>0 && (!"000".equals(pageNo)))
						    {
						    	pageNumAddress=calculatePositionOfSlottedPage(Integer.parseInt(pageNo.trim()));
						    	position=pageNumAddress+508;
						    	rm.seek(position);
						    	addBuff=new byte[3];
						    	rm.read(addBuff, 0, addBuff.length);
						    	lastByteLoc=new String(addBuff);
						    	if(lastByteLoc.trim().length()>0)
						    	{
						    		ReadDiskPageAddress rd=new ReadDiskPageAddress();
						    		rd.setPageNumber(pageNo);
						    		rd.setLastBytePointerLocation(lastByteLoc.trim());
						    		list.add(rd);
						    		position=pageNumAddress + 505;
						    	}
						    }
						    else
						    	break;
						}
						
					    for (ReadDiskPageAddress rd : list)
					    {
							ReadDiskPageAddress readDiskPageAddress = rd;
							pageNum= Integer.parseInt(rd.getPageNumber().trim());
							pageNumAddress=calculatePositionOfSlottedPage(pageNum);
							bytecount=Integer.parseInt(rd.getLastBytePointerLocation());
							int myCount=bytecount/16;
							position=pageNumAddress-1;
							for(int i=0;i<myCount;i++)
							{			
								position= pageNumAddress-1 + 16 *i;
								rm.seek(position);
								nameBuff=new byte[16];
								rm.read(nameBuff, 0, nameBuff.length);
								nameVal=new String(nameBuff);
								returnList.add(nameVal.trim());
							}
						}
					    rm.close();
						
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
				else
					if( AttributeName.toLowerCase().equals("phone"))
						{
							rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.phone, "rw");
							
							try 
							{
								byte[] phoneBuff;
								String phoneVal;
								//position= Integer.parseInt(base);
								while(true)
								{
									rm.seek(position);	
									byte[] pageNumber=new byte[3];
									rm.read(pageNumber, 0, pageNumber.length);
									val=new String(pageNumber);
									pageNo=val.trim();
								    if(pageNo.length()>0 && (!"000".equals(pageNo)))
								    {
								    	pageNumAddress=calculatePositionOfSlottedPage(Integer.parseInt(pageNo));
								    	position=pageNumAddress+508;
								    	rm.seek(position);
								    	addBuff=new byte[3];
								    	rm.read(addBuff, 0, addBuff.length);
								    	lastByteLoc=new String(addBuff);
								    	if(lastByteLoc.trim().length()>0)
								    	{
								    		ReadDiskPageAddress rd=new ReadDiskPageAddress();
								    		rd.setPageNumber(pageNo);
								    		rd.setLastBytePointerLocation(lastByteLoc.trim());
								    		list.add(rd);
								    		position=pageNumAddress + 505;
								    	}
								    }
								    else
								    	break;
								}
								
							    for (ReadDiskPageAddress rd : list)
							    {
									ReadDiskPageAddress readDiskPageAddress = rd;
									pageNum= Integer.parseInt(rd.getPageNumber().trim());
									pageNumAddress=calculatePositionOfSlottedPage(pageNum);
									bytecount=Integer.parseInt(rd.getLastBytePointerLocation().trim());
									int myCount=bytecount/12;
									position=pageNumAddress-1;
									for(int i=0;i<myCount;i++)
									{			
										position= pageNumAddress-1 + 12 *i;
										rm.seek(position);
										phoneBuff=new byte[12];
										rm.read(phoneBuff, 0, phoneBuff.length);
										phoneVal=new String(phoneBuff);
										returnList.add(phoneVal.trim());
									}
								}
							    rm.close();
								
							} catch (NumberFormatException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

						}				
			}
			else
			{
				//Return null if the table does not exist
				/*returnList.add("AAA");
				returnList.add("AAA");
				returnList.add("AAA");
				returnList.add("AAA");
				returnList.add("AAA");
				returnList.add("AAA");
				returnList.add("AAA");
				returnList.add("AAA");
				returnList.add("AAA");
				returnList.add("AAA");
				returnList.add("AAA");
				returnList.add("AAA");*/
				return null;
				
			}		
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return returnList;
	}
	
	public boolean ifTableExists(String TableName)
	{
		boolean response=false;
		if(tableName.contains(TableName.toUpperCase()))
		{
			response=true;
		}
		
		return response;
	}
	
	public void insertMMRowStorePages(String TableName, MMRowStorePages page,MetaMMR MetaRows)
	{
		List<Schema> list=new ArrayList<Schema>();
		
		for(int index=0; index<16;index++)
		{
			if(MetaRows.isNewEntry[index]==true)
			{
				Schema sc=new Schema();
				sc.setId(Integer.parseInt(page.ID[index]));
				sc.setName(page.Name[index]);
				sc.setPhone(page.PhoneNo[index]);
				list.add(sc);
			}
		}		
		
		if(list.size()>0)
		{
			if(tableName.contains(TableName.toUpperCase()))
			{
				this.newList=list;
				insertData(TableName.toUpperCase());
			}
			else
			{
				this.newList=list;
				this.tableName.add(TableName.toUpperCase());
				formHeaders(TableName.toUpperCase());
			    insertData(TableName.toUpperCase());
			}
			
		}
	}
	
	
	public HashMap<Integer, List<String>> getSlottedList(String TableName, String AttributeName)
	{
		HashMap<Integer, List<String>> bucketMap= new HashMap<Integer, List<String>>(); 		

		String pageNum;		
		RandomAccessFile rm;
		int pageStart,position, myCount,bytecount, pageno;
		byte[] slottedPageNumber,addBuff,idBuff,nameBuff,phoneBuff;
		String sltPageNum, addressByte,idVal,nameVal,phoneVal,lastByteLoc;
		
		if(tableName.contains(TableName.toUpperCase()) && AttributeName.toLowerCase().equals("id"))
		{
			File f=new File(baseFileLoc+ TableName.toUpperCase() +this.id);
			if(!f.exists())
			{
				return null;
			}
			else
			{
				try 
				{
					rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() +this.id, "r");
					for(int i=1; i<=16;i++)
					{
						position=7 * i;
						try 
						{							
							List<ReadDiskPageAddress> list=new ArrayList<ReadDiskPageAddress>();						
							while(true)
							{
								rm.seek(position);	
								slottedPageNumber=new byte[3];
								rm.read(slottedPageNumber, 0, slottedPageNumber.length);
								sltPageNum=new String(slottedPageNumber);
								pageNum=sltPageNum.trim();
							    if(pageNum.length()>0 && (!"000".equals(pageNum)))
							    {
							    	pageStart=calculatePositionOfSlottedPage(Integer.parseInt(pageNum));
							    	position=pageStart+508;
							    	rm.seek(position);
							    	addBuff=new byte[3];
							    	rm.read(addBuff, 0, addBuff.length);
							    	lastByteLoc=new String(addBuff);
							    	if(lastByteLoc.trim().length()>0)
							    	{
							    		ReadDiskPageAddress rd=new ReadDiskPageAddress();
							    		rd.setPageNumber(pageNum);
							    		rd.setLastBytePointerLocation(lastByteLoc);
							    		list.add(rd);
							    		position=pageStart + 505;
							    	}
							    }
							    else
							    	break;
							}
							
							//List<List<String>> bucketPageList=new ArrayList<List<String>>();
							List<String> pageList=new ArrayList<String>();
							for (ReadDiskPageAddress rd : list)
						    {
								ReadDiskPageAddress readDiskPageAddress = rd;
								pageno= Integer.parseInt(readDiskPageAddress.getPageNumber().trim());
								pageStart=calculatePositionOfSlottedPage(pageno);
								bytecount=Integer.parseInt(readDiskPageAddress.getLastBytePointerLocation().trim());
								myCount=bytecount/4;
								position=pageStart-1;
								
								for(int j=0;j<myCount;j++)
								{			
									position= pageStart-1 + 4 *j;
									rm.seek(position);
									idBuff=new byte[4];
									rm.read(idBuff, 0, idBuff.length);
									idVal=new String(idBuff);
									pageList.add(idVal.trim());
								}
								//bucketPageList.add(pageList);
							}
						    bucketMap.put(i, pageList);
							
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
					
					try {
						rm.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		else
			if(tableName.contains(TableName.toUpperCase()) && AttributeName.toLowerCase().equals("name"))
			{
				File f=new File(baseFileLoc+ TableName.toUpperCase() +this.name);
				if(!f.exists())
				{
					return null;
				}
				else
				{
					try 
					{
						rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() +this.name, "r");
						for(int i=1; i<=16;i++)
						{
							position=7 * i;
							try 
							{							
								List<ReadDiskPageAddress> nameList=new ArrayList<ReadDiskPageAddress>();
								nameList.clear();
								while(true)
								{
									rm.seek(position);	
									slottedPageNumber=new byte[3];
									rm.read(slottedPageNumber, 0, slottedPageNumber.length);
									sltPageNum=new String(slottedPageNumber);
									pageNum=sltPageNum.trim();
								    if(pageNum.length()>0 && (!"000".equals(pageNum)))
								    {
								    	pageStart=calculatePositionOfSlottedPage(Integer.parseInt(pageNum));
								    	position=pageStart+508;
								    	rm.seek(position);
								    	addBuff=new byte[3];
								    	rm.read(addBuff, 0, addBuff.length);
								    	lastByteLoc=new String(addBuff);
								    	if(lastByteLoc.trim().length()>0)
								    	{
								    		ReadDiskPageAddress rd=new ReadDiskPageAddress();
								    		rd.setPageNumber(pageNum);
								    		rd.setLastBytePointerLocation(lastByteLoc);
								    		nameList.add(rd);
								    		position=pageStart + 505;
								    	}
								    }
								    else
								    	break;
								}
								
								//List<List<String>> bucketPageList=new ArrayList<List<String>>();
								List<String> pageList=new ArrayList<String>();
								for (ReadDiskPageAddress rd : nameList)
							    {
									if(rd!=null)
									{
										ReadDiskPageAddress readDiskPageAddress = rd;
										pageno= Integer.parseInt(readDiskPageAddress.getPageNumber().trim());
										pageStart=calculatePositionOfSlottedPage(pageno);
										bytecount=Integer.parseInt(readDiskPageAddress.getLastBytePointerLocation().trim());
										myCount=bytecount/16;
										position=pageStart-1;
										
										for(int j=0;j<myCount;j++)
										{			
											position= pageStart-1 + 16 *j;
											rm.seek(position);
											nameBuff=new byte[16];
											rm.read(nameBuff, 0, nameBuff.length);
											nameVal=new String(nameBuff);
											pageList.add(nameVal.trim());
										}
										//bucketPageList.add(pageList);
									
									}
								}
							    bucketMap.put(i, pageList);
								
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
						}
						
						try {
							rm.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
			else
				if(tableName.contains(TableName.toUpperCase()) && AttributeName.toLowerCase().equals("phone"))
				{
					File f=new File(baseFileLoc+ TableName.toUpperCase() + this.phone);
					if(!f.exists())
					{
						return null;
					}
					else
					{
						try 
						{
							rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.phone, "r");
							for(int i=1; i<=16;i++)
							{
								position=7 * i;
								try 
								{							
									List<ReadDiskPageAddress> list=new ArrayList<ReadDiskPageAddress>();						
									while(true)
									{
										rm.seek(position);	
										slottedPageNumber=new byte[3];
										rm.read(slottedPageNumber, 0, slottedPageNumber.length);
										sltPageNum=new String(slottedPageNumber);
										pageNum=sltPageNum.trim();
									    if(pageNum.length()>0 && (!"000".equals(pageNum)))
									    {
									    	pageStart=calculatePositionOfSlottedPage(Integer.parseInt(pageNum));
									    	position=pageStart+508;
									    	rm.seek(position);
									    	addBuff=new byte[3];
									    	rm.read(addBuff, 0, addBuff.length);
									    	lastByteLoc=new String(addBuff);
									    	if(lastByteLoc.trim().length()>0)
									    	{
									    		ReadDiskPageAddress rd=new ReadDiskPageAddress();
									    		rd.setPageNumber(pageNum);
									    		rd.setLastBytePointerLocation(lastByteLoc);
									    		list.add(rd);
									    		position=pageStart + 505;
									    	}
									    }
									    else
									    	break;
									}
									
									//List<List<String>> bucketPageList=new ArrayList<List<String>>();
									List<String> pageList=new ArrayList<String>();
									for (ReadDiskPageAddress rd : list)
								    {
										ReadDiskPageAddress readDiskPageAddress = rd;
										pageno= Integer.parseInt(readDiskPageAddress.getPageNumber().trim());
										pageStart=calculatePositionOfSlottedPage(pageno);
										bytecount=Integer.parseInt(readDiskPageAddress.getLastBytePointerLocation().trim());
										myCount=bytecount/12;
										position=pageStart-1;
										
										for(int j=0;j<myCount;j++)
										{			
											position= pageStart-1 + 12 *j;
											rm.seek(position);
											phoneBuff=new byte[12];
											rm.read(phoneBuff, 0, phoneBuff.length);
											phoneVal=new String(phoneBuff);
											pageList.add(phoneVal.trim());
										}
										//bucketPageList.add(pageList);
									}
								    bucketMap.put(i, pageList);
									
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								
							}
							
							try {
								rm.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
						
				}
		return bucketMap;
	}
	
	@SuppressWarnings("resource")
	public List<String> getSlottedPageForBucketId(String TableName,String AttributeName, int BucketNum, int rowIndex)
	{
		List<String> slottedPageList=new ArrayList<String>();
		String division;
		int pageIndex,pageStart,position,byteCount;
		RandomAccessFile rm;
		byte[] addBuff, idBuff,nameBuff, phoneBuff,pageBuff;
		String addVal, idVal,nameVal,phoneVal, address;
		if(tableName.contains(TableName.toUpperCase()))
		{
			if(AttributeName.toLowerCase().equals("id"))
			{
				
				try 
				{
					rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.id, "r");
					if(rowIndex>4)
					{
						division=String.format("%2.04f", (float)rowIndex/4);
						String[] tempArray=division.split("\\.");
						pageIndex= Integer.parseInt(tempArray[0]);
						int num=Integer.parseInt(tempArray[1]);
						if(num>0)
						{
							pageIndex=pageIndex+1;
						}
					}
					else
						pageIndex=1;
					//Get the page start of the 4th page for the given bucket
					position=7* BucketNum;
					//position=pageStart-1;
					try 
					{
						
						rm.seek(position);	
						pageBuff=new byte[3];
						rm.read(pageBuff, 0, pageBuff.length);
						String pageVal=new String(pageBuff);
						if(pageVal.trim().length()>0 && !"000".equals(pageVal.trim()))
						{													
							int counter=0;
							while(true)
							{
								pageStart=calculatePositionOfSlottedPage(Integer.parseInt(pageVal.trim()));
								position=pageStart + 505 ;
								addBuff=new byte[3];
								rm.seek(position);
								rm.read(addBuff, 0, addBuff.length);
								addVal=new String(addBuff);
								if(addVal.trim().length()>0)
								{
									
									counter++;
									if(counter==pageIndex)
										break;
									else
									{   
										pageVal=addVal;
										continue;
									}
								}
								else
									break;
							}
							pageStart=calculatePositionOfSlottedPage(Integer.parseInt(pageVal.trim()));
							//position=pageStart-1;
							position=pageStart-1 + 509;
							rm.seek(position);
							addBuff=new byte[3];
							rm.read(addBuff, 0, addBuff.length);
							addVal=new String(addBuff);
							address=addVal.trim();
							if(address.length()>0)
							{
								byteCount=Integer.parseInt(address)/4;
								rm.seek(position);
								for (int l = 0; l < byteCount; l++) 
								{
									position= pageStart-1 + 4 *l;
									rm.seek(position);
									idBuff=new byte[4];
									rm.read(idBuff, 0, idBuff.length);
									idVal=new String(idBuff);
									slottedPageList.add(idVal.trim());
								}
								
							}
						}																					
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
				
				
			}
			else
				if(tableName.contains(TableName.toUpperCase()))
				{
					
					try {
						rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.name, "r");
						if(rowIndex>31)
						{
							division=String.format("%2.04f", (float)rowIndex/31);
							String[] tempArray=division.split("\\.");
							pageIndex= Integer.parseInt(tempArray[0]);
							int num=Integer.parseInt(tempArray[1]);
							if(num>0)
							{
								pageIndex=pageIndex+1;
							}
						}
						else
							pageIndex=1;
						//Get the page start of the 4th page for the given bucket
						position=7* BucketNum;
						//position=pageStart-1;
						try 
						{
							
							rm.seek(position);	
							pageBuff=new byte[3];
							rm.read(pageBuff, 0, pageBuff.length);
							String pageVal=new String(pageBuff);
							if(pageVal.trim().length()>0 && !"000".equals(pageVal.trim()))
							{													
								int counter=0;
								while(true)
								{
									pageStart=calculatePositionOfSlottedPage(Integer.parseInt(pageVal.trim()));
									position=pageStart + 505 ;
									addBuff=new byte[3];
									rm.seek(position);
									rm.read(addBuff, 0, addBuff.length);
									addVal=new String(addBuff);
									if(addVal.trim().length()>0)
									{
										
										counter++;
										if(counter==pageIndex)
											break;
										else
										{   
											pageVal=addVal;
											continue;
										}
									}
									else
										break;
								}
								pageStart=calculatePositionOfSlottedPage(Integer.parseInt(pageVal.trim()));
								//position=pageStart-1;
								position=pageStart-1 + 509;
								rm.seek(position);
								addBuff=new byte[3];
								rm.read(addBuff, 0, addBuff.length);
								addVal=new String(addBuff);
								address=addVal.trim();
								if(address.length()>0)
								{
									byteCount=Integer.parseInt(address)/16;
									rm.seek(position);
									for (int l = 0; l < byteCount; l++) 
									{
										position= pageStart-1 + 16 *l;
										rm.seek(position);
										nameBuff=new byte[16];
										rm.read(nameBuff, 0, nameBuff.length);
										nameVal=new String(nameBuff);
										slottedPageList.add(nameVal.trim());
									}
									
								}
							}																					
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
					
				}
				else
					if(tableName.contains(TableName.toUpperCase()))
					{
						try {
							rm=new RandomAccessFile(baseFileLoc+ TableName.toUpperCase() + this.phone, "r");
							if(rowIndex>42)
							{
								division=String.format("%2.04f", (float)rowIndex/42);
								String[] tempArray=division.split("\\.");
								pageIndex= Integer.parseInt(tempArray[0]);
								int num=Integer.parseInt(tempArray[1]);
								if(num>0)
								{
									pageIndex=pageIndex+1;
								}
							}
							else
								pageIndex=1;
							//Get the page start of the 4th page for the given bucket
							position=7* BucketNum;
							//position=pageStart-1;
							try 
							{
								
								rm.seek(position);	
								pageBuff=new byte[3];
								rm.read(pageBuff, 0, pageBuff.length);
								String pageVal=new String(pageBuff);
								if(pageVal.trim().length()>0 && !"000".equals(pageVal.trim()))
								{													
									int counter=0;
									while(true)
									{
										pageStart=calculatePositionOfSlottedPage(Integer.parseInt(pageVal.trim()));
										position=pageStart + 505 ;
										addBuff=new byte[3];
										rm.seek(position);
										rm.read(addBuff, 0, addBuff.length);
										addVal=new String(addBuff);
										if(addVal.trim().length()>0)
										{
											
											counter++;
											if(counter==pageIndex)
												break;
											else
											{   
												pageVal=addVal;
												continue;
											}
										}
										else
											break;
									}
									pageStart=calculatePositionOfSlottedPage(Integer.parseInt(pageVal.trim()));
									//position=pageStart-1;
									position=pageStart-1 + 509;
									rm.seek(position);
									addBuff=new byte[3];
									rm.read(addBuff, 0, addBuff.length);
									addVal=new String(addBuff);
									address=addVal.trim();
									if(address.length()>0)
									{
										byteCount=Integer.parseInt(address)/12;
										rm.seek(position);
										for (int l = 0; l < byteCount; l++) 
										{
											position= pageStart-1 + 12 *l;
											rm.seek(position);
											phoneBuff=new byte[12];
											rm.read(phoneBuff, 0, phoneBuff.length);
											phoneVal=new String(phoneBuff);
											slottedPageList.add(phoneVal.trim());
										}
										
									}
								}																					
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}												
			}
		}
		return slottedPageList;
	}
	
	public void deleteTable(String TableName) 
	{
		//File fid=new File("");
		if(tableName.contains(TableName.toUpperCase()))
		{
			File fid=new File(baseFileLoc + TableName.toUpperCase() + this.id);
			if(fid.exists())
				fid.delete();
			File fname=new File(baseFileLoc + TableName.toUpperCase() + this.name);
			if(fname.exists())
				fname.delete();
			File fphone=new File(baseFileLoc + TableName.toUpperCase() + this.phone);
			if(fphone.exists())
				fphone.delete();
			
			tableName.remove(TableName.toUpperCase());
			
			File meta=new File(metadataFile);
			if(meta.exists())
			{
				//meta.delete();
			}
			try 
			{
				meta.createNewFile();
				FileWriter fw = new FileWriter(meta.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw);
				for (String table: tableName) 
				{
					
					bw.write(table + "-id.txt" );
					bw.newLine();
					bw.write(table + "-name.txt" );
					bw.newLine();
					bw.write(table + "-phone.txt");
					bw.newLine();
				}
				bw.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void logWriter(String logentry)
	{

		try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("D:\\DBTest\\logfile.txt", true)))) 
		{
	
			out.println(logentry);
		}
		catch (IOException e) {

		}
	}

	public List<String> readScript(String ScriptName)
	{
		
		List<String> scriptList=new ArrayList<>();
		try 
		{		
			BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(baseFileLoc + ScriptName),"Cp1252"));			
			String line;
			
			try 
			{
				while((line=br.readLine())!=null)
				{
					if(!line.trim().equals("") && !line.trim().equals(null))
						scriptList.add(line);
				}
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		} catch (UnsupportedEncodingException e) {
			
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		}
		
		return scriptList;
	}

	//Inserts a Row into the disk
	public void insertRow(String TableName, int Id, String Name, String Phone)
	{
		List<Schema> list=new ArrayList<Schema>();
		Schema sh= new Schema();
		sh.setId(Id);
		sh.setName(Name);
		sh.setPhone(Phone);
		list.add(sh);
		insertData(TableName.toUpperCase());
	}
	
	public static void main(String agrs[])throws IOException
	{
		WriteToDisk w=new WriteToDisk();
		Scanner sc = new Scanner(System.in);
		
		File meta=new File(w.metadataFile);
		if(meta.exists())
		{
			BufferedReader br;
			try 
			{
				br = new BufferedReader(new FileReader(meta));
				String line;
				try {
					while ((line = br.readLine()) != null) 
					{
						
					   String[] temp=line.split("-");
	                   if(w.tableName.contains(temp[0].toUpperCase()))
	                   {
	                          
	                   }
	                   else
	                   {
	                          w.tableName.add(temp[0].toUpperCase());
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
		}
		else
		{
			int fileCount;
			while(true)
			{
				System.out.println("Please enter the count of data files:");
				fileCount= sc.nextInt();
				if(fileCount>0)
					break;
			}
			
			List<String> fileList=new ArrayList<String>();
			for (int i = 0; i < fileCount; i++) 
			{
				fileList.add(sc.next());
			}
			w.files=fileList;

			for (String iterator : fileList)
			{
				String[] nameArray=iterator.split(".txt");
				
				String fileId= w.baseFileLoc+ iterator + w.id;
				String fileName=w.baseFileLoc+ iterator + w.name;
				String filePhone=w.baseFileLoc+ iterator + w.phone;
					
				File fid=new File(fileId);
				File fname=new File(fileName);
				File fphone=new File(filePhone);
				if(fid.exists())
					fid.delete();
				if(fname.exists())
					fname.delete();
				if(fphone.exists())
					fphone.delete();
				
				if(!w.tableName.contains(nameArray[0].toUpperCase()))
				{
					w.tableName.add(nameArray[0].toUpperCase());
					w.newList=w.readFile(iterator);
					w.formHeaders(nameArray[0].toUpperCase());
					w.insertData(nameArray[0].toUpperCase());
				}
				
								
			}
		}		
	}

}


	


