import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

public class ConcurrencyReader
{
	
	public void logWriter(String logentry)
	{
		try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("D:\\DBTest\\logfile.txt", true)))) {
		    out.println(logentry);
		}catch (IOException e) {
		    //exception handling left as an exercise for the reader
		}
	}
	
	public int checkForDataAvailability(String operation, boolean isProcess,List<TransProc> tpList,int currentTransactionNumber, WaitForGraph wfg )
	{
		//Jesse reads the operation, defines if it is Process or a Transaction using the boolean field isProcess
		//After, an operation acquires a lock on a specific data item, it should add an object of LockItem in the List<LockItems> in TransProc
		//LockItem  should have the following
		//1.Operation i.e. either a R,I,D,M OR G.
		//2.TableName
		//3.A Schema Object: Filling up this object
						  //a. For I,R: Fill up the Id, name, phone
		String split[]= StringUtils.split(operation," (,)");
		String TableName=split[1];
		int flag=0,newFlag=0;
		if(isProcess==true)
		{
			if(split[0].equals("I"))
			{
				for(int tpIndex=0; tpIndex<tpList.size();tpIndex++)
				{
					TransProc tp=tpList.get(tpIndex);
					List<LockItems> LockList=tp.getLockItem();
					if(tp.isProcess==true)
					{
						for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
						{
							LockItems lock=LockList.get(lockIndex);
							if(lock.operation.equals("I") )
							{
								if(lock.TableName.equals(TableName))
								{
									int id= lock.schema.getId();
									if(id==Integer.parseInt(split[2]))
									{
										
										if(currentTransactionNumber!=tp.scriptNum)
										{											
											flag=1;
											wfg.setLink(currentTransactionNumber,tp.scriptNum);
											ArrayList<Integer> deadlockList= wfg.findDeadlock();
											if(deadlockList.size()>0)
											{
												Random myRandomizer = new Random();
												int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
												logWriter("Deadlock found. Aborting transaction on script "+index+".");
												wfg.resetNode(index);
												for(TransProc k:tpList)
												{
													if(k.scriptNum==currentTransactionNumber)
													{
														k.skipToTransactionEnd();
													}
												}
												System.out.println("\nDeadlock");
											}
											break;
										}
										else // if same operation already exists in the same process before.
										{
											int thisIndex= lockIndex;
											for(int newCounter=thisIndex; newCounter<LockList.size(); newCounter++)
											{
												LockItems newItem=new LockItems();
												if(newItem.operation.equals("D") && newItem.TableName.equals(TableName))
												{
													//flag=0;
													newFlag=1;
													break;
												}
											}
											if(newFlag==0)
												flag=-1;
											break;
										}							
									}
								}							
							}
							else
								if(lock.operation.equals("D") )
								{
									if(lock.TableName.equals(TableName))
									{
										if(currentTransactionNumber!=tp.scriptNum)
										{											
											flag=1;
											wfg.setLink(currentTransactionNumber,tp.scriptNum);
											ArrayList<Integer> deadlockList= wfg.findDeadlock();
											if(deadlockList.size()>0)
											{
												Random myRandomizer = new Random();
												int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
												logWriter("Deadlock found. Aborting transaction on script "+index+".");
												wfg.resetNode(index);
												for(TransProc k:tpList)
												{
													if(k.scriptNum==currentTransactionNumber)
													{
														k.skipToTransactionEnd();
													}
												}
												System.out.println("\nDeadlock");
											}
											
											break;
										}//What if script is same?
										else
										{
											//flag=0;
											newFlag=1;
											break;
										}
									}
								}								
						}
					}
					else //Transaction
					{
						for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
						{
							LockItems lock=LockList.get(lockIndex);
							if(lock.operation.equals("I") )
							{
								if(lock.TableName.equals(TableName))
								{
									int id= lock.schema.getId();
									if(id==Integer.parseInt(split[2]))
									{																											
										flag=1;
										wfg.setLink(currentTransactionNumber,tp.scriptNum);
										ArrayList<Integer> deadlockList= wfg.findDeadlock();
										if(deadlockList.size()>0)
										{
											Random myRandomizer = new Random();
											int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
											logWriter("Deadlock found. Aborting transaction on script "+index+".");
											wfg.resetNode(index);
											for(TransProc k:tpList)
											{
												if(k.scriptNum==currentTransactionNumber)
												{
													k.skipToTransactionEnd();
												}
											}
											System.out.println("\nDeadlock");
										}
									
										break;
									}
								}							
							}
							else
								if(lock.operation.equals("D") )
								{
									if(lock.TableName.equals(TableName))
									{
										flag=1;
										wfg.setLink(currentTransactionNumber,tp.scriptNum);
										ArrayList<Integer> deadlockList= wfg.findDeadlock();
										if(deadlockList.size()>0)
										{
											Random myRandomizer = new Random();
											int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
											logWriter("Deadlock found. Aborting transaction on script "+index+".");
											wfg.resetNode(index);
											for(TransProc k:tpList)
											{
												if(k.scriptNum==currentTransactionNumber)
												{
													k.skipToTransactionEnd();
												}
											}
											System.out.println("\nDeadlock");
										}
										break;
									}
								}	
								else
									if(lock.operation.equals("R"))
									{
										if(lock.TableName.equals(TableName))
										{
											int id= lock.schema.getId();
											if(id==Integer.parseInt(split[2]))
											{										
												flag=1;
												wfg.setLink(currentTransactionNumber,tp.scriptNum);
												ArrayList<Integer> deadlockList= wfg.findDeadlock();
												if(deadlockList.size()>0)
												{
													Random myRandomizer = new Random();
													int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
													logWriter("Deadlock found. Aborting transaction on script "+index+".");
													wfg.resetNode(index);
													for(TransProc k:tpList)
													{
														if(k.scriptNum==currentTransactionNumber)
														{
															k.skipToTransactionEnd();
														}
													}
													System.out.println("\nDeadlock");
												}
												break;
											}
										}
									}
									else
										if(lock.operation.equals("M"))
										{
											if(lock.TableName.equals(TableName))
										    {
												//For phone number in the lock list
												String phone= lock.schema.getPhone();
												String phArray[]=StringUtils.split(phone,"-");
												String areaCode=phArray[0];
												//For phone number of the current transaction that wants to acquire a lock
												String ph[]= StringUtils.split(split[4],"-");
												if(areaCode.equals(ph[0]))
												{
													flag=1;
													wfg.setLink(currentTransactionNumber,tp.scriptNum);
													ArrayList<Integer> deadlockList= wfg.findDeadlock();
													if(deadlockList.size()>0)
													{
														Random myRandomizer = new Random();
														int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
														logWriter("Deadlock found. Aborting transaction on script "+index+".");
														wfg.resetNode(index);
														for(TransProc k:tpList)
														{
															if(k.scriptNum==currentTransactionNumber)
															{
																k.skipToTransactionEnd();
															}
														}
														System.out.println("\nDeadlock");
													}
													break;
												}
										    }
										}
										else
											if(lock.operation.equals("G"))
											{
												if(lock.TableName.equals(TableName))
											    {
													//For phone number in the lock list
													String phone= lock.schema.getPhone();
													String phArray[]=StringUtils.split(phone,"-");
													String areaCode=phArray[0];
													//For phone number of the current transaction that wants to acquire a lock
													String ph[]= StringUtils.split(split[4],"-");
													if(areaCode.equals(ph[0]))
													{
														flag=1;
														wfg.setLink(currentTransactionNumber,tp.scriptNum);
														ArrayList<Integer> deadlockList= wfg.findDeadlock();
														if(deadlockList.size()>0)
														{
															Random myRandomizer = new Random();
															int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
															logWriter("Deadlock found. Aborting transaction on script "+index+".");
															wfg.resetNode(index);
															for(TransProc k:tpList)
															{
																if(k.scriptNum==currentTransactionNumber)
																{
																	k.skipToTransactionEnd();
																}
															}
															System.out.println("\nDeadlock");
														}
														break;
													}
											    }
											}
						}
					}
					
					if(flag!=0 || newFlag==1)
						break;
				}
			}
			else
				if(split[0].equals("R"))
				{
					for(int tpIndex=0; tpIndex<tpList.size();tpIndex++)
					{
						TransProc tp=tpList.get(tpIndex);
						List<LockItems> LockList=tp.getLockItem();
						if(tp.isProcess==true)
						{
							for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
							{
								LockItems lock=LockList.get(lockIndex);
								if(lock.operation.equals("I"))
								{
									if(lock.TableName.equals(TableName))
									{
										int id= lock.schema.getId();
										if(id==Integer.parseInt(split[2]))
										{											
											if(currentTransactionNumber!=tp.scriptNum)
											{											
												flag=1;
												wfg.setLink(currentTransactionNumber,tp.scriptNum);
												ArrayList<Integer> deadlockList= wfg.findDeadlock();
												if(deadlockList.size()>0)
												{
													Random myRandomizer = new Random();
													int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
													logWriter("Deadlock found. Aborting transaction on script "+index+".");
													wfg.resetNode(index);
													for(TransProc k:tpList)
													{
														if(k.scriptNum==currentTransactionNumber)
														{
															k.skipToTransactionEnd();
														}
													}
													System.out.println("\nDeadlock");
												}
												break;
											}
											
										}
									}
								}
								else
									if(lock.operation.equals("D"))
									{
										if(lock.TableName.equals(TableName))
										{
											if(currentTransactionNumber!=tp.scriptNum)
											{											
												flag=1;
												wfg.setLink(currentTransactionNumber,tp.scriptNum);
												ArrayList<Integer> deadlockList= wfg.findDeadlock();
												if(deadlockList.size()>0)
												{
													Random myRandomizer = new Random();
													int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
													wfg.resetNode(index);
													for(TransProc k:tpList)
													{
														if(k.scriptNum==currentTransactionNumber)
														{
															k.skipToTransactionEnd();
														}
													}
													System.out.println("\nDeadlock");
												}
												break;
											}
										}
									}
							}
						}
						else//transaction
						{
							for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
							{
								LockItems lock=LockList.get(lockIndex);
								if(lock.operation.equals("I"))
								{
									if(lock.TableName.equals(TableName))
									{
										int id= lock.schema.getId();
										if(id==Integer.parseInt(split[2]))
										{											
											flag=1;
											wfg.setLink(currentTransactionNumber,tp.scriptNum);
											ArrayList<Integer> deadlockList= wfg.findDeadlock();
											if(deadlockList.size()>0)
											{
												Random myRandomizer = new Random();
												int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
												logWriter("Deadlock found. Aborting transaction on script "+index+".");
												wfg.resetNode(index);
												for(TransProc k:tpList)
												{
													if(k.scriptNum==currentTransactionNumber)
													{
														k.skipToTransactionEnd();
													}
												}
												System.out.println("\nDeadlock");
											}
											break;
										}
									}
								}
								else
									if(lock.operation.equals("D"))
									{
										if(lock.TableName.equals(TableName))
										{
											flag=1;
											wfg.setLink(currentTransactionNumber,tp.scriptNum);
											ArrayList<Integer> deadlockList= wfg.findDeadlock();
											if(deadlockList.size()>0)
											{
												Random myRandomizer = new Random();
												int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
												logWriter("Deadlock found. Aborting transaction on script "+index+".");
												wfg.resetNode(index);
												for(TransProc k:tpList)
												{
													if(k.scriptNum==currentTransactionNumber)
													{
														k.skipToTransactionEnd();
													}
												}
												System.out.println("\nDeadlock");
											}
											break;
										}
									}
							}
						}
						if(flag!=0)
							break;
					}
				}
				else
					if(split[0].equals("D"))
					{
						for(int tpIndex=0; tpIndex<tpList.size();tpIndex++)
						{
							TransProc tp=tpList.get(tpIndex);
							List<LockItems> LockList=tp.getLockItem();
							if(tp.isProcess==true)
							{
								for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
								{
									LockItems lock=LockList.get(lockIndex);
									if(lock.operation.equals("I"))
									{
										if(lock.TableName.equals(TableName))
										{
											if(currentTransactionNumber!=tp.scriptNum)
											{											
												flag=1;
												wfg.setLink(currentTransactionNumber,tp.scriptNum);
												ArrayList<Integer> deadlockList= wfg.findDeadlock();
												if(deadlockList.size()>0)
												{
													Random myRandomizer = new Random();
													int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
													logWriter("Deadlock found. Aborting transaction on script "+index+".");
													wfg.resetNode(index);
													for(TransProc k:tpList)
													{
														if(k.scriptNum==currentTransactionNumber)
														{
															k.skipToTransactionEnd();
														}
													}
													System.out.println("\nDeadlock");
												}
												break;
											}//if same process has an Insert on that table
											else
											{
												//flag=0;
												newFlag=1;
												break;
											}
										}
									}
									else
										if(lock.operation.equals("D"))
										{
											if(lock.TableName.equals(TableName))
											{
												if(currentTransactionNumber!=tp.scriptNum)
												{											
													flag=1;
													wfg.setLink(currentTransactionNumber,tp.scriptNum);
													ArrayList<Integer> deadlockList= wfg.findDeadlock();
													if(deadlockList.size()>0)
													{
														Random myRandomizer = new Random();
														int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
														logWriter("Deadlock found. Aborting transaction on script "+index+".");
														wfg.resetNode(index);
														for(TransProc k:tpList)
														{
															if(k.scriptNum==currentTransactionNumber)
															{
																k.skipToTransactionEnd();
															}
														}
														System.out.println("\nDeadlock");
													}
													break;
												}
												else
												{
													//flag=-1;
													int thisIndex= lockIndex;
													for(int newCounter=thisIndex; newCounter<LockList.size(); newCounter++)
													{
														LockItems newItem=new LockItems();
														if(newItem.operation.equals("I") && newItem.TableName.equals(TableName))
														{
															//flag=0;
															newFlag=1;
															break;
														}
													}
													if(newFlag==0)
														flag=-1;
													break;
												}												
											}
										}										
								}
							}
							else//Transaction
							{
								for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
								{
									LockItems lock=LockList.get(lockIndex);
									if(lock.operation.equals("I"))
									{
										if(lock.TableName.equals(TableName))
										{
											flag=1;
											wfg.setLink(currentTransactionNumber,tp.scriptNum);
											ArrayList<Integer> deadlockList= wfg.findDeadlock();
											if(deadlockList.size()>0)
											{
												Random myRandomizer = new Random();
												int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
												logWriter("Deadlock found. Aborting transaction on script "+index+".");
												wfg.resetNode(index);
												for(TransProc k:tpList)
												{
													if(k.scriptNum==currentTransactionNumber)
													{
														k.skipToTransactionEnd();
													}
												}
												System.out.println("\nDeadlock");
											}
											break;
										}
									}
									else
										if(lock.operation.equals("R"))
										{
											if(lock.TableName.equals(TableName))
											{
												flag=1;
												wfg.setLink(currentTransactionNumber,tp.scriptNum);
												ArrayList<Integer> deadlockList= wfg.findDeadlock();
												if(deadlockList.size()>0)
												{
													Random myRandomizer = new Random();
													int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
													logWriter("Deadlock found. Aborting transaction on script "+index+".");
													wfg.resetNode(index);
													for(TransProc k:tpList)
													{
														if(k.scriptNum==currentTransactionNumber)
														{
															k.skipToTransactionEnd();
														}
													}
													System.out.println("\nDeadlock");
												}
												break;
											}
										}
										else
											if(lock.operation.equals("D"))
											{
												if(lock.TableName.equals(TableName))
												{
													flag=1;
													wfg.setLink(currentTransactionNumber,tp.scriptNum);
													ArrayList<Integer> deadlockList= wfg.findDeadlock();
													if(deadlockList.size()>0)
													{
														Random myRandomizer = new Random();
														int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
														logWriter("Deadlock found. Aborting transaction on script "+index+".");
														wfg.resetNode(index);
														for(TransProc k:tpList)
														{
															if(k.scriptNum==currentTransactionNumber)
															{
																k.skipToTransactionEnd();
															}
														}
														System.out.println("\nDeadlock");
													}
													break;
												}
											}
											else
												if(lock.operation.equals("M"))
												{
													if(lock.TableName.equals(TableName))
													{
														flag=1;
														wfg.setLink(currentTransactionNumber,tp.scriptNum);
														ArrayList<Integer> deadlockList= wfg.findDeadlock();
														if(deadlockList.size()>0)
														{
															Random myRandomizer = new Random();
															int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
															logWriter("Deadlock found. Aborting transaction on script "+index+".");
															wfg.resetNode(index);
															for(TransProc k:tpList)
															{
																if(k.scriptNum==currentTransactionNumber)
																{
																	k.skipToTransactionEnd();
																}
															}
															System.out.println("\nDeadlock");
														}
														break;
													}
												}
												else
													if(lock.operation.equals("G"))
													{
														if(lock.TableName.equals(TableName))
														{
															flag=1;
															wfg.setLink(currentTransactionNumber,tp.scriptNum);
															ArrayList<Integer> deadlockList= wfg.findDeadlock();
															if(deadlockList.size()>0)
															{
																Random myRandomizer = new Random();
																int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
																logWriter("Deadlock found. Aborting transaction on script "+index+".");
																wfg.resetNode(index);
																for(TransProc k:tpList)
																{
																	if(k.scriptNum==currentTransactionNumber)
																	{
																		k.skipToTransactionEnd();
																	}
																}
																System.out.println("\nDeadlock");
															}
															break;
														}
													}										
								}								
							}
							if(flag!=0 || newFlag==1)
								break;
						}
					}
					else
						if(split[0].equals("M"))
						{
							for(int tpIndex=0; tpIndex<tpList.size();tpIndex++)
							{
								TransProc tp=tpList.get(tpIndex);
								List<LockItems> LockList=tp.getLockItem();
								if(tp.isProcess==true)
								{
									for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
									{
										LockItems lock=LockList.get(lockIndex);
										if(lock.operation.equals("I"))
										{
											if(lock.TableName.equals(TableName))
											{
												//For phone number in the lock list
												String phone= lock.schema.getPhone();
												String phArray[]=StringUtils.split(phone,"-");
												String areaCode=phArray[0];
												//For phone number of the current transaction that wants to acquire a lock
												String ph= split[2]; //StringUtils.split(split[4],"-");
												if(areaCode.equals(ph))
												{
													if(currentTransactionNumber!=tp.scriptNum)
													{											
														flag=1;
														wfg.setLink(currentTransactionNumber,tp.scriptNum);
														ArrayList<Integer> deadlockList= wfg.findDeadlock();
														if(deadlockList.size()>0)
														{
															Random myRandomizer = new Random();
															int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
															logWriter("Deadlock found. Aborting transaction on script "+index+".");
															wfg.resetNode(index);
															for(TransProc k:tpList)
															{
																if(k.scriptNum==currentTransactionNumber)
																{
																	k.skipToTransactionEnd();
																}
															}
															System.out.println("\nDeadlock");
														}
														break;
													}																							
												}
											}
										}
										else
											if(lock.operation.equals("D"))
											{
												if(lock.TableName.equals(TableName))
												{
													if(currentTransactionNumber!=tp.scriptNum)
													{											
														flag=1;
														wfg.setLink(currentTransactionNumber,tp.scriptNum);
														ArrayList<Integer> deadlockList= wfg.findDeadlock();
														if(deadlockList.size()>0)
														{
															Random myRandomizer = new Random();
															int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
															logWriter("Deadlock found. Aborting transaction on script "+index+".");
															wfg.resetNode(index);
															for(TransProc k:tpList)
															{
																if(k.scriptNum==currentTransactionNumber)
																{
																	k.skipToTransactionEnd();
																}
															}
															System.out.println("\nDeadlock");
														}
														break;
													}																																				
												}
											}											
									}
								}
								else//Transaction
								{
									for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
									{
										LockItems lock=LockList.get(lockIndex);
										if(lock.operation.equals("I"))
										{
											if(lock.TableName.equals(TableName))
											{
												//For phone number in the lock list
												String phone= lock.schema.getPhone();
												String phArray[]=StringUtils.split(phone,"-");
												String areaCode=phArray[0];
												//For phone number of the current transaction that wants to acquire a lock
												String ph= split[2] ; //StringUtils.split(split[4],"-");
												if(areaCode.equals(ph))
												{
													flag=1;
													wfg.setLink(currentTransactionNumber,tp.scriptNum);
													ArrayList<Integer> deadlockList= wfg.findDeadlock();
													if(deadlockList.size()>0)
													{
														Random myRandomizer = new Random();
														int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
														logWriter("Deadlock found. Aborting transaction on script "+index+".");
														wfg.resetNode(index);
														for(TransProc k:tpList)
														{
															if(k.scriptNum==currentTransactionNumber)
															{
																k.skipToTransactionEnd();
															}
														}
														System.out.println("\nDeadlock");
													}
													break;																																			
												}
											}
										}
										else
											if(lock.operation.equals("D"))
											{
												if(lock.TableName.equals(TableName))
												{
													flag=1;
													wfg.setLink(currentTransactionNumber,tp.scriptNum);
													ArrayList<Integer> deadlockList= wfg.findDeadlock();
													if(deadlockList.size()>0)
													{
														Random myRandomizer = new Random();
														int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
														logWriter("Deadlock found. Aborting transaction on script "+index+".");
														wfg.resetNode(index);
														for(TransProc k:tpList)
														{
															if(k.scriptNum==currentTransactionNumber)
															{
																k.skipToTransactionEnd();
															}
														}
														System.out.println("\nDeadlock");
													}
													break;																																																	
												}
											}	
									}
								}
								if(flag!=0)
									break;
							}
						}
						else
							if(split[0].equals("G"))
							{
								for(int tpIndex=0; tpIndex<tpList.size();tpIndex++)
								{
									TransProc tp=tpList.get(tpIndex);
									List<LockItems> LockList=tp.getLockItem();
									if(tp.isProcess==true)
									{
										for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
										{
											LockItems lock=LockList.get(lockIndex);
											if(lock.operation.equals("I"))
											{
												if(lock.TableName.equals(TableName))
												{
													//For phone number in the lock list
													String phone= lock.schema.getPhone();
													String phArray[]=StringUtils.split(phone,"-");
													String areaCode=phArray[0];
													//For phone number of the current transaction that wants to acquire a lock
													String ph= split[2];// StringUtils.split(split[4],"-");
													if(areaCode.equals(ph))
													{
														if(currentTransactionNumber!=tp.scriptNum)
														{
															flag=1;
															wfg.setLink(currentTransactionNumber,tp.scriptNum);
															ArrayList<Integer> deadlockList= wfg.findDeadlock();
															if(deadlockList.size()>0)
															{
																Random myRandomizer = new Random();
																int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
																logWriter("Deadlock found. Aborting transaction on script "+index+".");
																wfg.resetNode(index);
																for(TransProc k:tpList)
																{
																	if(k.scriptNum==currentTransactionNumber)
																	{
																		k.skipToTransactionEnd();
																	}
																}
																System.out.println("\nDeadlock");
															}
															break;	
														}																																	
													}
												}
											}
											else
												if(lock.operation.equals("D"))
												{
													if(lock.TableName.equals(TableName))
													{
														if(currentTransactionNumber!=tp.scriptNum)
														{											
															flag=1;
															wfg.setLink(currentTransactionNumber,tp.scriptNum);
															ArrayList<Integer> deadlockList= wfg.findDeadlock();
															if(deadlockList.size()>0)
															{
																Random myRandomizer = new Random();
																int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
																logWriter("Deadlock found. Aborting transaction on script "+index+".");
																wfg.resetNode(index);
																for(TransProc k:tpList)
																{
																	if(k.scriptNum==currentTransactionNumber)
																	{
																		k.skipToTransactionEnd();
																	}
																}
																System.out.println("\nDeadlock");
															}
															break;
														}																																				
													}
												}	
										}
									}
									else//Transaction
									{
										for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
										{
											LockItems lock=LockList.get(lockIndex);
											if(lock.operation.equals("I"))
											{
												if(lock.operation.equals("I"))
												{
													if(lock.TableName.equals(TableName))
													{
														//For phone number in the lock list
														String phone= lock.schema.getPhone();
														String phArray[]=StringUtils.split(phone,"-");
														String areaCode=phArray[0];
														//For phone number of the current transaction that wants to acquire a lock
														String ph=split[2]; // StringUtils.split(split[4],"-");
														if(areaCode.equals(ph))
														{
															flag=1;
															wfg.setLink(currentTransactionNumber,tp.scriptNum);
															ArrayList<Integer> deadlockList= wfg.findDeadlock();
															if(deadlockList.size()>0)
															{
																Random myRandomizer = new Random();
																int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
																logWriter("Deadlock found. Aborting transaction on script "+index+".");
																wfg.resetNode(index);
																for(TransProc k:tpList)
																{
																	if(k.scriptNum==currentTransactionNumber)
																	{
																		k.skipToTransactionEnd();
																	}
																}
																System.out.println("\nDeadlock");
															}
															break;																																			
														}
													}
												}
												else
													if(lock.operation.equals("D"))
													{
														if(lock.TableName.equals(TableName))
														{
															flag=1;
															wfg.setLink(currentTransactionNumber,tp.scriptNum);
															ArrayList<Integer> deadlockList= wfg.findDeadlock();
															if(deadlockList.size()>0)
															{
																Random myRandomizer = new Random();
																int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
																logWriter("Deadlock found. Aborting transaction on script "+index+".");
																wfg.resetNode(index);
																for(TransProc k:tpList)
																{
																	if(k.scriptNum==currentTransactionNumber)
																	{
																		k.skipToTransactionEnd();
																	}
																}
																System.out.println("\nDeadlock");
															}
															break;																																																	
														}
													}
											}
										}
									}
									if(flag!=0)
										break;
								}
							}
		}
		else //For Current Transaction
		{
			if(split[0].equals("I"))
			{
				for(int tpIndex=0; tpIndex<tpList.size();tpIndex++)
				{
					TransProc tp=tpList.get(tpIndex);
					List<LockItems> LockList=tp.getLockItem();
					if(tp.isProcess==true)
					{
						for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
						{
							LockItems lock=LockList.get(lockIndex);
							if(lock.operation.equals("I"))
							{
								if(lock.TableName.equals(TableName))
								{																										
									int id= lock.schema.getId();
									if(id==Integer.parseInt(split[2]))
									{									
										flag=1;
										wfg.setLink(currentTransactionNumber,tp.scriptNum);
										ArrayList<Integer> deadlockList= wfg.findDeadlock();
										if(deadlockList.size()>0)
										{
											Random myRandomizer = new Random();
											int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
											logWriter("Deadlock found. Aborting transaction on script "+index+".");
											wfg.resetNode(index);
											for(TransProc k:tpList)
											{
												if(k.scriptNum==currentTransactionNumber)
												{
													k.skipToTransactionEnd();
												}
											}
											System.out.println("\nDeadlock");
										}
										break;
									}
								}
							}
							else
								if(lock.operation.equals("D"))
								{
									if(lock.TableName.equals(TableName))
									{
										flag=1;
										wfg.setLink(currentTransactionNumber,tp.scriptNum);
										ArrayList<Integer> deadlockList= wfg.findDeadlock();
										if(deadlockList.size()>0)
										{
											Random myRandomizer = new Random();
											int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
											logWriter("Deadlock found. Aborting transaction on script "+index+".");
											wfg.resetNode(index);
											for(TransProc k:tpList)
											{
												if(k.scriptNum==currentTransactionNumber)
												{
													k.skipToTransactionEnd();
												}
											}
											System.out.println("\nDeadlock");
										}
										break;
									}
								}
								
						}
					}
					else//Transaction
					{
						for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
						{
							LockItems lock=LockList.get(lockIndex);
							if(lock.operation.equals("I"))
							{
								if(lock.TableName.equals(TableName))
								{
									int id= lock.schema.getId();
									if(id==Integer.parseInt(split[2]))
									{			
										if(currentTransactionNumber!=tp.scriptNum)
										{
											flag=1;
											wfg.setLink(currentTransactionNumber,tp.scriptNum);
											ArrayList<Integer> deadlockList= wfg.findDeadlock();
											if(deadlockList.size()>0)
											{
												Random myRandomizer = new Random();
												int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
												logWriter("Deadlock found. Aborting transaction on script "+index+".");
												wfg.resetNode(index);
												for(TransProc k:tpList)
												{
													if(k.scriptNum==currentTransactionNumber)
													{
														k.skipToTransactionEnd();
													}
												}
												System.out.println("\nDeadlock");
											}
											break;
										}
										else
										{
											int thisIndex= lockIndex;
											for(int newCounter=thisIndex; newCounter<LockList.size(); newCounter++)
											{
												LockItems newItem=new LockItems();
												if(newItem.operation.equals("D") && newItem.TableName.equals(TableName))
												{
													//flag=0;
													newFlag=1;
													break;
												}
											}
											if(newFlag==0)
												flag=-1;
											break;
										}
										
									}
								}
							}
							else
								if(lock.operation.equals("D"))
								{
									if(lock.TableName.equals(TableName))
									{
										if(currentTransactionNumber!=tp.scriptNum)
										{
											flag=1;
											wfg.setLink(currentTransactionNumber,tp.scriptNum);
											ArrayList<Integer> deadlockList= wfg.findDeadlock();
											if(deadlockList.size()>0)
											{
												Random myRandomizer = new Random();
												int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
												logWriter("Deadlock found. Aborting transaction on script "+index+".");
												wfg.resetNode(index);
												for(TransProc k:tpList)
												{
													if(k.scriptNum==currentTransactionNumber)
													{
														k.skipToTransactionEnd();
													}
												}
												System.out.println("\nDeadlock");
											}
											break;
										}//What if same transaction?
										else
										{
											newFlag=1;
											break;
										}
									}
								}
								else
									if(lock.operation.equals("R"))
									{
										if(lock.TableName.equals(TableName))
										{
											int id= lock.schema.getId();
											if(id==Integer.parseInt(split[2]))
											{		
												if(currentTransactionNumber!=tp.scriptNum)
												{
													flag=1;
													wfg.setLink(currentTransactionNumber,tp.scriptNum);
													ArrayList<Integer> deadlockList= wfg.findDeadlock();
													if(deadlockList.size()>0)
													{
														Random myRandomizer = new Random();
														int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
														logWriter("Deadlock found. Aborting transaction on script "+index+".");
														wfg.resetNode(index);
														for(TransProc k:tpList)
														{
															if(k.scriptNum==currentTransactionNumber)
															{
																k.skipToTransactionEnd();
															}
														}
														System.out.println("\nDeadlock");
													}
													break;
												}
												
											}
										}
									}
									else
										if(lock.operation.equals("M"))
										{
											if(lock.TableName.equals(TableName))
										    {
												//For phone number in the lock list
												String phone= lock.schema.getPhone();
												String phArray[]=StringUtils.split(phone,"-");
												String areaCode=phArray[0];
												//For phone number of the current transaction that wants to acquire a lock
												String ph[]= StringUtils.split(split[4],"-");
												if(areaCode.equals(ph[0]))
												{
													if(currentTransactionNumber!=tp.scriptNum)
													{
														flag=1;
														wfg.setLink(currentTransactionNumber,tp.scriptNum);
														ArrayList<Integer> deadlockList= wfg.findDeadlock();
														if(deadlockList.size()>0)
														{
															Random myRandomizer = new Random();
															int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
															logWriter("Deadlock found. Aborting transaction on script "+index+".");
															wfg.resetNode(index);
															for(TransProc k:tpList)
															{
																if(k.scriptNum==currentTransactionNumber)
																{
																	k.skipToTransactionEnd();
																}
															}
															System.out.println("\nDeadlock");
														}
														break;
													}													
												}
										    }
										}
										else
											if(lock.operation.equals("G"))
											{
												if(lock.TableName.equals(TableName))
											    {
													//For phone number in the lock list
													String phone= lock.schema.getPhone();
													String phArray[]=StringUtils.split(phone,"-");
													String areaCode=phArray[0];
													//For phone number of the current transaction that wants to acquire a lock
													String ph[]= StringUtils.split(split[4],"-");
													if(areaCode.equals(ph[0]))
													{
														if(currentTransactionNumber!=tp.scriptNum)
														{
															flag=1;
															wfg.setLink(currentTransactionNumber,tp.scriptNum);
															ArrayList<Integer> deadlockList= wfg.findDeadlock();
															if(deadlockList.size()>0)
															{
																Random myRandomizer = new Random();
																int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
																logWriter("Deadlock found. Aborting transaction on script "+index+".");
																wfg.resetNode(index);
																for(TransProc k:tpList)
																{
																	if(k.scriptNum==currentTransactionNumber)
																	{
																		k.skipToTransactionEnd();
																	}
																}
																System.out.println("\nDeadlock");
															}
															break;
														}
														
													}
											    }
											}
						}
					}
					if(flag!=0 || newFlag==1)
						break;
				}
			}
			else
				if(split[0].equals("R"))
				{
					for(int tpIndex=0; tpIndex<tpList.size();tpIndex++)
					{
						TransProc tp=tpList.get(tpIndex);
						List<LockItems> LockList=tp.getLockItem();
						if(tp.isProcess==true)
						{
							for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
							{
								LockItems lock=LockList.get(lockIndex);
								if(lock.operation.equals("I"))
								{
									if(lock.TableName.equals(TableName))
									{
										int id= lock.schema.getId();
										if(id==Integer.parseInt(split[2]))
										{			
											flag=1;
											wfg.setLink(currentTransactionNumber,tp.scriptNum);
											ArrayList<Integer> deadlockList= wfg.findDeadlock();
											if(deadlockList.size()>0)
											{
												Random myRandomizer = new Random();
												int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
												logWriter("Deadlock found. Aborting transaction on script "+index+".");
												wfg.resetNode(index);
												for(TransProc k:tpList)
												{
													if(k.scriptNum==currentTransactionNumber)
													{
														k.skipToTransactionEnd();
													}
												}
												System.out.println("\nDeadlock");
											}
											break;																			
										}
									}
								}
								else
									if(lock.operation.equals("D"))
									{
										if(lock.TableName.equals(TableName))
										{
											flag=1;
											wfg.setLink(currentTransactionNumber,tp.scriptNum);
											ArrayList<Integer> deadlockList= wfg.findDeadlock();
											if(deadlockList.size()>0)
											{
												Random myRandomizer = new Random();
												int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
												logWriter("Deadlock found. Aborting transaction on script "+index+".");
												wfg.resetNode(index);
												for(TransProc k:tpList)
												{
													if(k.scriptNum==currentTransactionNumber)
													{
														k.skipToTransactionEnd();
													}
												}
												System.out.println("\nDeadlock");
											}
											break;
										}
									}
							}
						}
						else//Transaction
						{
							for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
							{
								LockItems lock=LockList.get(lockIndex);
								if(lock.operation.equals("I"))
								{
									if(lock.TableName.equals(TableName))
									{
										int id= lock.schema.getId();
										if(id==Integer.parseInt(split[2]))
										{																
											if(currentTransactionNumber!=tp.scriptNum)
											{
												flag=1;
												wfg.setLink(currentTransactionNumber,tp.scriptNum);
												ArrayList<Integer> deadlockList= wfg.findDeadlock();
												if(deadlockList.size()>0)
												{
													Random myRandomizer = new Random();
													int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
													logWriter("Deadlock found. Aborting transaction on script "+index+".");
													wfg.resetNode(index);
													for(TransProc k:tpList)
													{
														if(k.scriptNum==currentTransactionNumber)
														{
															k.skipToTransactionEnd();
														}
													}
													System.out.println("\nDeadlock");
												}
												break;
											}
										}
											
									}
								}
								else
									if(lock.operation.equals("D"))
									{
										if(lock.TableName.equals(TableName))
										{
											if(currentTransactionNumber!=tp.scriptNum)
											{
												flag=1;
												wfg.setLink(currentTransactionNumber,tp.scriptNum);
												ArrayList<Integer> deadlockList= wfg.findDeadlock();
												if(deadlockList.size()>0)
												{
													Random myRandomizer = new Random();
													int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
													logWriter("Deadlock found. Aborting transaction on script "+index+".");
													wfg.resetNode(index);
													for(TransProc k:tpList)
													{
														if(k.scriptNum==currentTransactionNumber)
														{
															k.skipToTransactionEnd();
														}
													}
													System.out.println("\nDeadlock");
												}
												break;
											}//Reader should check if same transaction has a delete operation on the same table
										}
									}
							}
						}
						if(flag!=0)
							break;
					}
				}
				else
					if(split[0].equals("D"))
					{
						for(int tpIndex=0; tpIndex<tpList.size();tpIndex++)
						{
							TransProc tp=tpList.get(tpIndex);
							List<LockItems> LockList=tp.getLockItem();
							if(tp.isProcess==true)
							{
								for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
								{
									LockItems lock=LockList.get(lockIndex);
									if(lock.operation.equals("I"))
									{
										if(lock.TableName.equals(TableName))
										{
											flag=1;
											wfg.setLink(currentTransactionNumber,tp.scriptNum);
											ArrayList<Integer> deadlockList= wfg.findDeadlock();
											if(deadlockList.size()>0)
											{
												Random myRandomizer = new Random();
												int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
												logWriter("Deadlock found. Aborting transaction on script "+index+".");
												wfg.resetNode(index);
												for(TransProc k:tpList)
												{
													if(k.scriptNum==currentTransactionNumber)
													{
														k.skipToTransactionEnd();
													}
												}
												System.out.println("\nDeadlock");
											}
											break;
										}
									}
									else
										if(lock.operation.equals("D"))
										{
											if(lock.TableName.equals(TableName))
											{
												flag=1;
												wfg.setLink(currentTransactionNumber,tp.scriptNum);
												ArrayList<Integer> deadlockList= wfg.findDeadlock();
												if(deadlockList.size()>0)
												{
													Random myRandomizer = new Random();
													int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
													logWriter("Deadlock found. Aborting transaction on script "+index+".");
													wfg.resetNode(index);
													for(TransProc k:tpList)
													{
														if(k.scriptNum==currentTransactionNumber)
														{
															k.skipToTransactionEnd();
														}
													}
													System.out.println("\nDeadlock");
												}
												break;
											}
										}
								}
							}
							else//Transaction
							{
								for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
								{
									LockItems lock=LockList.get(lockIndex);
									if(lock.operation.equals("I"))
									{
										if(lock.TableName.equals(TableName))
										{
											if(currentTransactionNumber!=tp.scriptNum)
											{
												flag=1;
												wfg.setLink(currentTransactionNumber,tp.scriptNum);
												ArrayList<Integer> deadlockList= wfg.findDeadlock();
												if(deadlockList.size()>0)
												{
													Random myRandomizer = new Random();
													int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
													logWriter("Deadlock found. Aborting transaction on script "+index+".");
													wfg.resetNode(index);
													for(TransProc k:tpList)
													{
														if(k.scriptNum==currentTransactionNumber)
														{
															k.skipToTransactionEnd();
														}
													}
													System.out.println("\nDeadlock");
												}
												break;
											}//When scripts are same //The scriptAnalyser should check for the prev insertions on the same table
											else
											{
												newFlag=1;
												break;
											}
										}
									}
									else
										if(lock.operation.equals("R"))
										{
											if(lock.TableName.equals(TableName))
											{
												if(currentTransactionNumber!=tp.scriptNum)
												{
													flag=1;
													wfg.setLink(currentTransactionNumber,tp.scriptNum);
													ArrayList<Integer> deadlockList= wfg.findDeadlock();
													if(deadlockList.size()>0)
													{
														Random myRandomizer = new Random();
														int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
														logWriter("Deadlock found. Aborting transaction on script "+index+".");
														wfg.resetNode(index);
														for(TransProc k:tpList)
														{
															if(k.scriptNum==currentTransactionNumber)
															{
																k.skipToTransactionEnd();
															}
														}
														System.out.println("\nDeadlock");
													}
													break;
												}//When scripts are same //The scriptAnalyser should check for the prev operations on the same table
											}
										}
										else
											if(lock.operation.equals("D"))
											{
												if(lock.TableName.equals(TableName))
												{
													if(currentTransactionNumber!=tp.scriptNum)
													{
														flag=1;
														wfg.setLink(currentTransactionNumber,tp.scriptNum);
														ArrayList<Integer> deadlockList= wfg.findDeadlock();
														if(deadlockList.size()>0)
														{
															Random myRandomizer = new Random();
															int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
															logWriter("Deadlock found. Aborting transaction on script "+index+".");
															wfg.resetNode(index);
															for(TransProc k:tpList)
															{
																if(k.scriptNum==currentTransactionNumber)
																{
																	k.skipToTransactionEnd();
																}
															}
															System.out.println("\nDeadlock");
														}
														break;
													}
													else
													{
														//flag=-1;
														int thisIndex= lockIndex;
														for(int newCounter=thisIndex; newCounter<LockList.size(); newCounter++)
														{
															LockItems newItem=new LockItems();
															if(newItem.operation.equals("I") && newItem.TableName.equals(TableName))
															{
																//flag=0;
																newFlag=1;
																break;
															}
														}
														if(newFlag==0)
															flag=-1;
														break;
													}
												}
											}
											else
												if(lock.operation.equals("M"))
												{
													if(lock.TableName.equals(TableName))
													{
														if(currentTransactionNumber!=tp.scriptNum)
														{
															flag=1;
															wfg.setLink(currentTransactionNumber,tp.scriptNum);
															ArrayList<Integer> deadlockList= wfg.findDeadlock();
															if(deadlockList.size()>0)
															{
																Random myRandomizer = new Random();
																int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
																logWriter("Deadlock found. Aborting transaction on script "+index+".");
																wfg.resetNode(index);
																for(TransProc k:tpList)
																{
																	if(k.scriptNum==currentTransactionNumber)
																	{
																		k.skipToTransactionEnd();
																	}
																}
																System.out.println("\nDeadlock");
															}
															break;
														}//ScriptAnalyzer should check for previous M & G
													}
												}
												else
													if(lock.operation.equals("G"))
													{
														if(lock.TableName.equals(TableName))
														{
															if(currentTransactionNumber!=tp.scriptNum)
															{
																flag=1;
																wfg.setLink(currentTransactionNumber,tp.scriptNum);
																ArrayList<Integer> deadlockList= wfg.findDeadlock();
																if(deadlockList.size()>0)
																{
																	Random myRandomizer = new Random();
																	int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
																	logWriter("Deadlock found. Aborting transaction on script "+index+".");
																	wfg.resetNode(index);
																	for(TransProc k:tpList)
																	{
																		if(k.scriptNum==currentTransactionNumber)
																		{
																			k.skipToTransactionEnd();
																		}
																	}
																	System.out.println("\nDeadlock");
																}
																break;
															}//ScriptAnalyzer should check for previous M & G
														}
													}
								}
							}
							if(flag!=0 ||newFlag==1)
								break;
						}
					}
					else
						if(split[0].equals("M"))
						{
							for(int tpIndex=0; tpIndex<tpList.size();tpIndex++)
							{
								TransProc tp=tpList.get(tpIndex);
								List<LockItems> LockList=tp.getLockItem();
								if(tp.isProcess==true)
								{
									for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
									{
										LockItems lock=LockList.get(lockIndex);
										if(lock.operation.equals("I"))
										{
											if(lock.TableName.equals(TableName))
											{
												//For phone number in the lock list
												String phone= lock.schema.getPhone();
												String phArray[]=StringUtils.split(phone,"-");
												String areaCode=phArray[0];
												//For phone number of the current transaction that wants to acquire a lock
												String ph= split[2]; //StringUtils.split(split[4],"-");
												if(areaCode.equals(ph))
												{
													flag=1;
													wfg.setLink(currentTransactionNumber,tp.scriptNum);
													ArrayList<Integer> deadlockList= wfg.findDeadlock();
													if(deadlockList.size()>0)
													{
														Random myRandomizer = new Random();
														int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
														logWriter("Deadlock found. Aborting transaction on script "+index+".");
														wfg.resetNode(index);
														for(TransProc k:tpList)
														{
															if(k.scriptNum==currentTransactionNumber)
															{
																k.skipToTransactionEnd();
															}
														}
														System.out.println("\nDeadlock");
													}
													break;																																		
												}
											}
										}
										else
											if(lock.operation.equals("D"))
											{
												if(lock.TableName.equals(TableName))
												{
													flag=1;
													wfg.setLink(currentTransactionNumber,tp.scriptNum);
													ArrayList<Integer> deadlockList= wfg.findDeadlock();
													if(deadlockList.size()>0)
													{
														Random myRandomizer = new Random();
														int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
														logWriter("Deadlock found. Aborting transaction on script "+index+".");
														wfg.resetNode(index);
														for(TransProc k:tpList)
														{
															if(k.scriptNum==currentTransactionNumber)
															{
																k.skipToTransactionEnd();
															}
														}
														System.out.println("\nDeadlock");
													}
													break;																																																
												}
											}
									}
								}
								else//Transaction
								{
									for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
									{
										LockItems lock=LockList.get(lockIndex);
										if(lock.operation.equals("I"))
										{
											if(lock.TableName.equals(TableName))
											{
												//For phone number in the lock list
												String phone= lock.schema.getPhone();
												String phArray[]=StringUtils.split(phone,"-");
												String areaCode=phArray[0];
												//For phone number of the current transaction that wants to acquire a lock
												String ph=split[2]; // StringUtils.split(split[4],"-");
												if(areaCode.equals(ph))
												{
													if(currentTransactionNumber!=tp.scriptNum)
													{
														flag=1;
														wfg.setLink(currentTransactionNumber,tp.scriptNum);
														ArrayList<Integer> deadlockList= wfg.findDeadlock();
														if(deadlockList.size()>0)
														{
															Random myRandomizer = new Random();
															int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
															logWriter("Deadlock found. Aborting transaction on script "+index+".");
															wfg.resetNode(index);
															for(TransProc k:tpList)
															{
																if(k.scriptNum==currentTransactionNumber)
																{
																	k.skipToTransactionEnd();
																}
															}
															System.out.println("\nDeadlock");
														}
														break;
													}																																															
												}
											}
										}
										else
											if(lock.operation.equals("D"))
											{
												if(lock.TableName.equals(TableName))
												{
													if(currentTransactionNumber!=tp.scriptNum)
													{
														flag=1;
														wfg.setLink(currentTransactionNumber,tp.scriptNum);
														ArrayList<Integer> deadlockList= wfg.findDeadlock();
														if(deadlockList.size()>0)
														{
															Random myRandomizer = new Random();
															int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
															logWriter("Deadlock found. Aborting transaction on script "+index+".");
															wfg.resetNode(index);
															for(TransProc k:tpList)
															{
																if(k.scriptNum==currentTransactionNumber)
																{
																	k.skipToTransactionEnd();
																}
															}
															System.out.println("\nDeadlock");
														}
														break;
													}																																																	
												}
											}
									}
								}
								if(flag!=0)
									break;
							}
						}
						else
							if(split[0].equals("G"))
							{
								for(int tpIndex=0; tpIndex<tpList.size();tpIndex++)
								{
									TransProc tp=tpList.get(tpIndex);
									List<LockItems> LockList=tp.getLockItem();
									if(tp.isProcess==true)
									{
										for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
										{
											LockItems lock=LockList.get(lockIndex);
											if(lock.operation.equals("I"))
											{
												if(lock.TableName.equals(TableName))
												{
													//For phone number in the lock list
													String phone= lock.schema.getPhone();
													String phArray[]=StringUtils.split(phone,"-");
													String areaCode=phArray[0];
													//For phone number of the current transaction that wants to acquire a lock
													String ph=split[2]; // StringUtils.split(split[4],"-");
													if(areaCode.equals(ph))
													{
														flag=1;
														wfg.setLink(currentTransactionNumber,tp.scriptNum);
														ArrayList<Integer> deadlockList= wfg.findDeadlock();
														if(deadlockList.size()>0)
														{
															Random myRandomizer = new Random();
															int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
															logWriter("Deadlock found. Aborting transaction on script "+index+".");
															wfg.resetNode(index);
															for(TransProc k:tpList)
															{
																if(k.scriptNum==currentTransactionNumber)
																{
																	k.skipToTransactionEnd();
																}
															}
															System.out.println("\nDeadlock");
														}
														break;																																			
													}
												}
											}
											else
												if(lock.operation.equals("D"))
												{
													if(lock.TableName.equals(TableName))
													{
														flag=1;
														wfg.setLink(currentTransactionNumber,tp.scriptNum);
														ArrayList<Integer> deadlockList= wfg.findDeadlock();
														if(deadlockList.size()>0)
														{
															Random myRandomizer = new Random();
															int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
															logWriter("Deadlock found. Aborting transaction on script "+index+".");
															wfg.resetNode(index);
															for(TransProc k:tpList)
															{
																if(k.scriptNum==currentTransactionNumber)
																{
																	k.skipToTransactionEnd();
																}
															}
															System.out.println("\nDeadlock");
														}
														break;																																																
													}
												}
										}
									}
									else//Transaction
									{
										for(int lockIndex=0;lockIndex<LockList.size();lockIndex++)
										{
											LockItems lock=LockList.get(lockIndex);
											if(lock.operation.equals("I"))
											{
												if(lock.TableName.equals(TableName))
												{
													//For phone number in the lock list
													String phone= lock.schema.getPhone();
													String phArray[]=StringUtils.split(phone,"-");
													String areaCode=phArray[0];
													//For phone number of the current transaction that wants to acquire a lock
													String ph=split[2] ; // StringUtils.split(split[4],"-");
													if(areaCode.equals(ph))
													{
														if(currentTransactionNumber!=tp.scriptNum)
														{
															flag=1;
															wfg.setLink(currentTransactionNumber,tp.scriptNum);
															ArrayList<Integer> deadlockList= wfg.findDeadlock();
															if(deadlockList.size()>0)
															{
																Random myRandomizer = new Random();
																int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
																logWriter("Deadlock found. Aborting transaction on script "+index+".");
																wfg.resetNode(index);
																for(TransProc k:tpList)
																{
																	if(k.scriptNum==currentTransactionNumber)
																	{
																		k.skipToTransactionEnd();
																	}
																}
																System.out.println("\nDeadlock");
															}
															break;
														}																																															
													}
												}
											}
											else
												if(lock.operation.equals("D"))
												{
													if(lock.TableName.equals(TableName))
													{
														if(currentTransactionNumber!=tp.scriptNum)
														{
															flag=1;
															wfg.setLink(currentTransactionNumber,tp.scriptNum);
															ArrayList<Integer> deadlockList= wfg.findDeadlock();
															if(deadlockList.size()>0)
															{
																Random myRandomizer = new Random();
																int index =deadlockList.get(myRandomizer.nextInt(deadlockList.size()));
																logWriter("Deadlock found. Aborting transaction on script "+index+".");
																wfg.resetNode(index);
																for(TransProc k:tpList)
																{
																	if(k.scriptNum==currentTransactionNumber)
																	{
																		k.skipToTransactionEnd();
																	}
																}
																System.out.println("\nDeadlock");
															}
															break;
														}																																																	
													}
												}
										}
									}
									if(flag!=0)
										break;
								}
							}
		}		
		return flag;
	}
}
