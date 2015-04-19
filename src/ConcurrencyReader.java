import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

public class ConcurrencyReader
{
	public int checkForDataAvailability(String operation, boolean isProcess,List<TransProc> tpList,int currentTransactionNumber )
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
		int flag=0;
		if(isProcess==true)
		{
			if(split[0].equals('I'))
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
										//setLink(currentTransactionNumber,tp.scriptNum);
										}
										else
											flag=-1;
										break;
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
										//setLink(currentTransactionNumber,tp.scriptNum);
											break;
										}//What if script is same?
										
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
										//setLink(currentTransactionNumber,tp.scriptNum);
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
										//setLink(currentTransactionNumber,tp.scriptNum);
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
												//setLink(currentTransactionNumber,tp.scriptNum);
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
												String phArray[]=StringUtils.split(phone);
												int areaCode=Integer.parseInt(phArray[0]);
												//For phone number of the current transaction that wants to acquire a lock
												String ph[]= StringUtils.split(split[4],"-");
												if(areaCode==Integer.parseInt(ph[0]))
												{
													flag=1;
													//setLink(currentTransactionNumber,tp.scriptNum);
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
													String phArray[]=StringUtils.split(phone);
													int areaCode=Integer.parseInt(phArray[0]);
													//For phone number of the current transaction that wants to acquire a lock
													String ph[]= StringUtils.split(split[4],"-");
													if(areaCode==Integer.parseInt(ph[0]))
													{
														flag=1;
														//setLink(currentTransactionNumber,tp.scriptNum);
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
				if(split[0].equals('R'))
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
											//setLink(currentTransactionNumber,tp.scriptNum);
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
											//setLink(currentTransactionNumber,tp.scriptNum);
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
											//setLink(currentTransactionNumber,tp.scriptNum);
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
											//setLink(currentTransactionNumber,tp.scriptNum);
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
					if(split[0].equals('D'))
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
											//setLink(currentTransactionNumber,tp.scriptNum);
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
												//setLink(currentTransactionNumber,tp.scriptNum);
												}
												else
													flag=-1;
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
											flag=1;
											//setLink(currentTransactionNumber,tp.scriptNum);
											break;
										}
									}
									else
										if(lock.operation.equals("R"))
										{
											if(lock.TableName.equals(TableName))
											{
												flag=1;
												//setLink(currentTransactionNumber,tp.scriptNum);
												break;
											}
										}
										else
											if(lock.operation.equals("D"))
											{
												if(lock.TableName.equals(TableName))
												{
													flag=1;
													//setLink(currentTransactionNumber,tp.scriptNum);
													break;
												}
											}
											else
												if(lock.operation.equals("M"))
												{
													if(lock.TableName.equals(TableName))
													{
														flag=1;
														//setLink(currentTransactionNumber,tp.scriptNum);
														break;
													}
												}
												else
													if(lock.operation.equals("G"))
													{
														if(lock.TableName.equals(TableName))
														{
															flag=1;
															//setLink(currentTransactionNumber,tp.scriptNum);
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
						if(split[0].equals('M'))
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
												String phArray[]=StringUtils.split(phone);
												int areaCode=Integer.parseInt(phArray[0]);
												//For phone number of the current transaction that wants to acquire a lock
												String ph[]= StringUtils.split(split[4],"-");
												if(areaCode==Integer.parseInt(ph[0]))
												{
													if(currentTransactionNumber!=tp.scriptNum)
													{											
														flag=1;
														//setLink(currentTransactionNumber,tp.scriptNum);
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
														//setLink(currentTransactionNumber,tp.scriptNum);
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
												String phArray[]=StringUtils.split(phone);
												int areaCode=Integer.parseInt(phArray[0]);
												//For phone number of the current transaction that wants to acquire a lock
												String ph[]= StringUtils.split(split[4],"-");
												if(areaCode==Integer.parseInt(ph[0]))
												{
													flag=1;
													//setLink(currentTransactionNumber,tp.scriptNum);
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
													//setLink(currentTransactionNumber,tp.scriptNum);
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
							if(split[0].equals('G'))
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
													String phArray[]=StringUtils.split(phone);
													int areaCode=Integer.parseInt(phArray[0]);
													//For phone number of the current transaction that wants to acquire a lock
													String ph[]= StringUtils.split(split[4],"-");
													if(areaCode==Integer.parseInt(ph[0]))
													{
														if(currentTransactionNumber!=tp.scriptNum)
														{
															flag=1;
															//setLink(currentTransactionNumber,tp.scriptNum);
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
															//setLink(currentTransactionNumber,tp.scriptNum);
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
														String phArray[]=StringUtils.split(phone);
														int areaCode=Integer.parseInt(phArray[0]);
														//For phone number of the current transaction that wants to acquire a lock
														String ph[]= StringUtils.split(split[4],"-");
														if(areaCode==Integer.parseInt(ph[0]))
														{
															flag=1;
															//setLink(currentTransactionNumber,tp.scriptNum);
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
															//setLink(currentTransactionNumber,tp.scriptNum);
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
			if(split[0].equals('I'))
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
										//setLink(currentTransactionNumber,tp.scriptNum);
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
										//setLink(currentTransactionNumber,tp.scriptNum);
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
											//setLink(currentTransactionNumber,tp.scriptNum);
										}
										else
											flag=-1;
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
											//setLink(currentTransactionNumber,tp.scriptNum);
											break;
										}//What if same transaction?
									}
								}
						}
					}
					if(flag!=0)
						break;
				}
			}
			else
				if(split[0].equals('R'))
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
											//setLink(currentTransactionNumber,tp.scriptNum);
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
											//setLink(currentTransactionNumber,tp.scriptNum);
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
												//setLink(currentTransactionNumber,tp.scriptNum);
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
												//setLink(currentTransactionNumber,tp.scriptNum);
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
					if(split[0].equals('D'))
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
											//setLink(currentTransactionNumber,tp.scriptNum);
											break;
										}
									}
									else
										if(lock.operation.equals("D"))
										{
											if(lock.TableName.equals(TableName))
											{
												flag=1;
												//setLink(currentTransactionNumber,tp.scriptNum);
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
												//setLink(currentTransactionNumber,tp.scriptNum);
												break;
											}//When scripts are same //The scriptAnalyser should check for the prev insertions on the same table
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
													//setLink(currentTransactionNumber,tp.scriptNum);
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
														//setLink(currentTransactionNumber,tp.scriptNum);
													}
													else
														flag=-1;
													break;
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
															//setLink(currentTransactionNumber,tp.scriptNum);
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
																//setLink(currentTransactionNumber,tp.scriptNum);
																break;
															}//ScriptAnalyzer should check for previous M & G
														}
													}
								}
							}
							if(flag!=0)
								break;
						}
					}
					else
						if(split[0].equals('M'))
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
												String phArray[]=StringUtils.split(phone);
												int areaCode=Integer.parseInt(phArray[0]);
												//For phone number of the current transaction that wants to acquire a lock
												String ph[]= StringUtils.split(split[4],"-");
												if(areaCode==Integer.parseInt(ph[0]))
												{
													flag=1;
													//setLink(currentTransactionNumber,tp.scriptNum);
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
													//setLink(currentTransactionNumber,tp.scriptNum);
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
												String phArray[]=StringUtils.split(phone);
												int areaCode=Integer.parseInt(phArray[0]);
												//For phone number of the current transaction that wants to acquire a lock
												String ph[]= StringUtils.split(split[4],"-");
												if(areaCode==Integer.parseInt(ph[0]))
												{
													if(currentTransactionNumber!=tp.scriptNum)
													{
														flag=1;
														//setLink(currentTransactionNumber,tp.scriptNum);
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
														//setLink(currentTransactionNumber,tp.scriptNum);
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
							if(split[0].equals('G'))
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
													String phArray[]=StringUtils.split(phone);
													int areaCode=Integer.parseInt(phArray[0]);
													//For phone number of the current transaction that wants to acquire a lock
													String ph[]= StringUtils.split(split[4],"-");
													if(areaCode==Integer.parseInt(ph[0]))
													{
														flag=1;
														//setLink(currentTransactionNumber,tp.scriptNum);
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
														//setLink(currentTransactionNumber,tp.scriptNum);
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
													String phArray[]=StringUtils.split(phone);
													int areaCode=Integer.parseInt(phArray[0]);
													//For phone number of the current transaction that wants to acquire a lock
													String ph[]= StringUtils.split(split[4],"-");
													if(areaCode==Integer.parseInt(ph[0]))
													{
														if(currentTransactionNumber!=tp.scriptNum)
														{
															flag=1;
															//setLink(currentTransactionNumber,tp.scriptNum);
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
															//setLink(currentTransactionNumber,tp.scriptNum);
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