package com.etl;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class EtlTester {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException, SQLException {

		Connection con = null;
		String aSQLScriptFilePath = "";
		List<String> dbObjects = new ArrayList<>();
		DataLoader dl=null;
		Properties properties = new Properties();
		File pf = new File("config.properties");
		properties.load(new FileReader(pf));
		String fileExtractURLTableName = properties.getProperty("FileExtractURLTableName");
		Map<String,String> scriptExecuteMap = new HashMap<>();
		SimpleDateFormat sdf = new SimpleDateFormat(properties.getProperty("DateTimeFormat"));
		try {
			validateCMDLineArgs(args);
			if(args[0].equalsIgnoreCase("RunStat")) {
				runStatisticsOnly(args,con);
			} else if(args[0].equalsIgnoreCase("Re-Process")) {
				System.out.println("DataLoader running in manual mode");

				String[] fileList = args[2].split(",");
				Utility.runID = Integer.valueOf(args[3]);
				Utility.applicationStart(true,args[1]);
				
				
				dl = new DataLoader("r",Utility.runID);
				dl.createDbExtract('r', fileList);

				if(!fileExtractURLTableName.isEmpty()) {
					String[] arr= fileExtractURLTableName.split(",");
					for(String urlDimension:arr) {
						if(Arrays.asList(fileList).contains(urlDimension)) {
							dl.createDbExtractFromURL();
						}
					}
				}
				List<String> failedFileList=new ArrayList<>(Arrays.asList(fileList)); 
				dl.doAmazonS3FileTransfer(failedFileList);


				String tmp = "";
				if(!dl.isDWEntryRestricted()) {
					aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
					if(aSQLScriptFilePath!=null && !aSQLScriptFilePath.isEmpty()) {
						File folder = new File(aSQLScriptFilePath);
						File[] listOfFiles = folder.listFiles();

						dbObjects.addAll(dl.getListOfDimensionsFacts());

						if(!dbObjects.isEmpty() && listOfFiles!=null) {
							for (String dbObject : dbObjects) {
								if( !dl.checkErrorStatus("Extraction", dbObject) && !dl.checkErrorStatus("Loading", dbObject)) {
									for (int i = 0; i < listOfFiles.length; i++) {
										tmp =  dbObject + "_prestage_opn.sql"; 
										long jobId = Utility.dbObjectJobIdMap.get(dbObject);
										if (tmp.equalsIgnoreCase(listOfFiles[i].getName())) {
											System.out.println("\n--------------------------------------------------------");
											System.out.println("Script Execution Started For " + dbObject);
											System.out.println("--------------------------------------------------------");

											if (proceedWithFactScriptExecution(dbObject,scriptExecuteMap)) {
												Class.forName(Utility.getConfig("RSCLASS"));
												con = DriverManager.getConnection(Utility.getConfig("RSDBURL"),	Utility.getConfig("RSUID"), Utility.getConfig("RSPWD"));

												ScriptRunner scriptRunner = new ScriptRunner(con, false, true, dbObject,scriptExecuteMap);

												scriptRunner.runScript(new FileReader(aSQLScriptFilePath + File.separator + listOfFiles[i].getName()),jobId,scriptExecuteMap);

												Utility.closeConnection(con);

												Utility.writeJobLog(jobId, "COMPLETED",	dl.getSdf().format(Calendar.getInstance().getTime())); 

												if(!Utility.isDimension(dbObject) && scriptExecuteMap.get(dbObject)==null ) { 
													Properties p = new Properties();
													File tmpf = new File("tmpFile.properties");
													p.load(new FileReader(tmpf));
													String lastModDate = p.getProperty(dbObject);
													String[] tmpDt = lastModDate.split(",");
													//	dl.updateFactsProperty("facts.properties", dbObject, lastModDate);
													Utility.updateFactExtDtl(dbObject, tmpDt[0], tmpDt[1], tmpDt[2]);
												}

												break;
											}
											else {
												throw new Exception("There is an issue in dimension's script execution, so fact script execution is skipped.");
											}
										} else {
											continue;
										}

									}
								}
							}
						}	

					}
				}
				Utility.writeLog("RunID " + Utility.runID + " Application has ended.", "Info", "", "Application Ends", "DB"); 
				System.out.println("\nRunID " + Utility.runID + " Application has ended.");
				
			} else if(args[0].equalsIgnoreCase("ErrProcess")) { 
			
				List<String> factList = new ArrayList<>(Arrays.asList(args[2].split(",")));
								
				if(args[1] != null) {
					
					String tmp = "";
					Utility.runID = Integer.valueOf(args[1]);
					Utility.applicationStart(true,"-99");
					aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
					if(aSQLScriptFilePath!=null && !aSQLScriptFilePath.isEmpty()) {
						File folder = new File(aSQLScriptFilePath);
						File[] listOfFiles = folder.listFiles();
						
						dbObjects.addAll(factList);
						if(!dbObjects.isEmpty() && listOfFiles!=null) {
							for (String dbObject : dbObjects) {
								for (int i = 0; i < listOfFiles.length; i++) {
									tmp =  dbObject + "_error_prestage_opn.sql"; 
									
									if (tmp.equalsIgnoreCase(listOfFiles[i].getName())) {
										System.out.println("\n--------------------------------------------------------");
										System.out.println("Script Execution Started For " + dbObject);
										System.out.println("--------------------------------------------------------");
										String content = new Scanner(new File(aSQLScriptFilePath + File.separator + listOfFiles[i].getName())).useDelimiter("\\Z").next();
										content = content.replace("RUN_ID_PLACEHOLDER", args[1]);
										PrintWriter pw =new PrintWriter(aSQLScriptFilePath + File.separator + listOfFiles[i].getName());
										pw.write(content);
										pw.close();
										
										Class.forName(Utility.getConfig("RSCLASS"));
										con = DriverManager.getConnection(Utility.getConfig("RSDBURL"),	Utility.getConfig("RSUID"),Utility.getConfig("RSPWD"));
										ScriptRunner scriptRunner = new ScriptRunner(con,false, true,dbObject,scriptExecuteMap);
										long jobId = Utility.writeJobLog(Utility.runID, dbObject, "ErrProcess", "In-Progress");
										Utility.writeJobLog(jobId, "REDSHIFTLOADSTART", sdf.format(Calendar.getInstance().getTime()));
										scriptRunner.runScript(new FileReader(aSQLScriptFilePath + File.separator	+ listOfFiles[i].getName()),jobId,scriptExecuteMap);
										Utility.writeJobLog(jobId, "REDSHIFTLOADEND", sdf.format(Calendar.getInstance().getTime()));
										Utility.closeConnection(con);

									} else {
										continue;
									}

								
							}
						}
						}
					} else {
						
						throw new Exception("Error!! SQLFile Location is not defined..");
					}
					
					
				} else {
					
					String tmp = "";
					Utility.runID = -99;
					Utility.applicationStart(true,"-99");
					aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
					if(aSQLScriptFilePath!=null && !aSQLScriptFilePath.isEmpty()) {
						File folder = new File(aSQLScriptFilePath);
						File[] listOfFiles = folder.listFiles();
						
						dbObjects.addAll(factList);
						if(!dbObjects.isEmpty() && listOfFiles!=null) {
							for (String dbObject : dbObjects) {
								for (int i = 0; i < listOfFiles.length; i++) {
									tmp =  dbObject + "_error_prestage_opn.sql"; 
									
									if (tmp.equalsIgnoreCase(listOfFiles[i].getName())) {
										System.out.println("\n--------------------------------------------------------");
										System.out.println("Script Execution Started For " + dbObject);
										System.out.println("--------------------------------------------------------");
										String content = new Scanner(new File(aSQLScriptFilePath + File.separator + listOfFiles[i].getName())).useDelimiter("\\Z").next();
										content = content.replace("RUN_ID_PLACEHOLDER", args[1]);
										PrintWriter pw =new PrintWriter(aSQLScriptFilePath + File.separator + listOfFiles[i].getName());
										pw.write(content);
										pw.close();
										
										Class.forName(Utility.getConfig("RSCLASS"));
										con = DriverManager.getConnection(Utility.getConfig("RSDBURL"),	Utility.getConfig("RSUID"),Utility.getConfig("RSPWD"));
										ScriptRunner scriptRunner = new ScriptRunner(con,false, true,dbObject,scriptExecuteMap);
										long jobId = Utility.writeJobLog(Utility.runID, dbObject, "ErrProcess", "In-Progress");
										Utility.writeJobLog(jobId, "REDSHIFTLOADSTART", sdf.format(Calendar.getInstance().getTime()));
										scriptRunner.runScript(new FileReader(aSQLScriptFilePath + File.separator	+ listOfFiles[i].getName()),jobId,scriptExecuteMap);
										Utility.writeJobLog(jobId, "REDSHIFTLOADEND", sdf.format(Calendar.getInstance().getTime()));
										Utility.closeConnection(con);

									} else {
										continue;
									}

								
							}
						}
						}
					} else {
						throw new Exception("Error!! SQLFile Location is not defined..");
						
					}
					
				}
				
				Utility.writeLog("Application has ended.", "Info", "", "Application Ends", "DB"); 
				System.out.println("Application has ended.");
			
			} else if(args[0].equalsIgnoreCase("Hierarchy")) {
				
				if(!args[1].isEmpty()) {
					List<String> dimList =new ArrayList<>(Arrays.asList(args[1].split(","))); 
					Utility.applicationStart(false,"-99");
					
					System.out.println("DataLoader running in Hierarchy mode");
					Utility.writeLog("DataLoader running in Hierarchy mode", "Info", "", "Application Startup in Hierarchy Mode", "DB"); 
					
					if(dimList.contains("employees")){
						
						System.out.println("Building Employees hierarchy..");
						Utility.writeLog("Building Employees hierarchy..", "Info", "Employees", "Employee_Hierarchy_Initialization", "DB"); 
						RelationerDataLoaderForEmployees rdlEmp = new RelationerDataLoaderForEmployees();
						rdlEmp.processResult();
						
						
					} 
					if(dimList.contains("subsidieries")) {
						
						System.out.println("Building Subsidieries hierarchy..");
						Utility.writeLog("Building Subsidieries hierarchy..", "Info", "Subsidieries", "Subsidieries_Hierarchy_Initialization", "DB");
						RelationerDataLoaderForSubsidiery rdlSub = new RelationerDataLoaderForSubsidiery();
						rdlSub.processResult();
						
					} 
					if(dimList.contains("locations")) {
						
						System.out.println("Building Locations hierarchy..");
						Utility.writeLog("Building Locations hierarchy..", "Info", "Locations", "Locations_Hierarchy_Initialization", "DB");
						RelationerDataLoaderForLocation rdlLoc = new RelationerDataLoaderForLocation();
						rdlLoc.processResult();
						
					}
					
					Utility.writeLog("RunID " + Utility.runID + " Application has ended.", "Info", "", "Application Ends", "DB"); 
					System.out.println("\nRunID " + Utility.runID + " Application has ended.");
					
				} else {
					
					throw new Exception("Wrong or Blank Argument Passed.. Program will now exit..");
				}
				
				
			} else {
				
				Utility.applicationStart(false,args[0]);
								
				String tmp = "";

				dl= new DataLoader();

				dl.createDbExtract(); 

				if(!fileExtractURLTableName.equals("")) {
					dl.createDbExtractFromURL();
				}	
				dl.doAmazonS3FileTransfer(); 
				if(!dl.isDWEntryRestricted()) {
					aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
					if(aSQLScriptFilePath!=null && !aSQLScriptFilePath.isEmpty()) {
						File folder = new File(aSQLScriptFilePath);
						File[] listOfFiles = folder.listFiles();

						dbObjects.addAll(dl.getListOfDimensionsFacts());

						if(!dbObjects.isEmpty() && listOfFiles!=null) {
							for (String dbObject : dbObjects) {
								if( !dl.checkErrorStatus("Extraction", dbObject) && !dl.checkErrorStatus("Loading", dbObject)) {
									for (int i = 0; i < listOfFiles.length; i++) {
										tmp =  dbObject + "_prestage_opn.sql"; 
										long jobId = Utility.dbObjectJobIdMap.get(dbObject);
										if (tmp.equalsIgnoreCase(listOfFiles[i].getName())) {
											System.out.println("\n--------------------------------------------------------");
											System.out.println("Script Execution Started For "+dbObject);
											System.out.println("--------------------------------------------------------");

											if( proceedWithFactScriptExecution(dbObject,scriptExecuteMap)) {
												Class.forName(Utility.getConfig("RSCLASS"));
												con = DriverManager.getConnection(Utility.getConfig("RSDBURL"),	Utility.getConfig("RSUID"),Utility.getConfig("RSPWD"));
												ScriptRunner scriptRunner = new ScriptRunner(con,false, true,dbObject,scriptExecuteMap);
												scriptRunner.runScript(new FileReader(aSQLScriptFilePath + File.separator	+ listOfFiles[i].getName()),jobId,scriptExecuteMap);
												Utility.closeConnection(con);
												Utility.writeJobLog(jobId, "COMPLETED",	dl.getSdf().format(Calendar.getInstance().getTime())); 

												if(!Utility.isDimension(dbObject) && scriptExecuteMap.get(dbObject)==null ) { 
													Properties p = new Properties();
													File tmpf = new File("tmpFile.properties");
													p.load(new FileReader(tmpf));
													String lastModDate = p.getProperty(dbObject);
													String[] tmpDt = lastModDate.split(",");
													//	dl.updateFactsProperty("facts.properties", dbObject, lastModDate);
													Utility.updateFactExtDtl(dbObject, tmpDt[0], tmpDt[1], tmpDt[2]);
												}

												break;
											} else {
												throw new Exception("There is an issue in dimension's script execution, so fact script execution is skipped.");
											}
										} else {
											continue;
										}

									}
								}
							}
						}	
					}
				}
				
				System.out.println("DataLoader running in Hierarchy mode");
				Utility.writeLog("DataLoader running in Hierarchy mode", "Info", "", "Application Startup in Hierarchy Mode", "DB"); 

				System.out.println("Building Employees hierarchy..");
				Utility.writeLog("Building Employees hierarchy..", "Info", "Employees", "Employee_Hierarchy_Initialization", "DB"); 
				RelationerDataLoaderForEmployees rdlEmp = new RelationerDataLoaderForEmployees();
				rdlEmp.processResult();

				System.out.println("Building Subsidieries hierarchy..");
				Utility.writeLog("Building Subsidieries hierarchy..", "Info", "Subsidieries", "Subsidieries_Hierarchy_Initialization", "DB");
				RelationerDataLoaderForSubsidiery rdlSub = new RelationerDataLoaderForSubsidiery();
				rdlSub.processResult();

				System.out.println("Building Locations hierarchy..");
				Utility.writeLog("Building Locations hierarchy..", "Info", "Locations", "Locations_Hierarchy_Initialization", "DB");
				RelationerDataLoaderForLocation rdlLoc = new RelationerDataLoaderForLocation();
				rdlLoc.processResult();
				
				Utility.writeLog("RunID " + Utility.runID + " Application has ended.", "Info", "", "Application Ends", "DB"); 
				System.out.println("\nRunID " + Utility.runID + " Application has ended.");
				if (Utility.proceedToRunStatistics()) {
					runStatistics(con, dl);
				}
			}

		} catch (ClassNotFoundException e) {
			System.out.println("Error !! Please check error message "+ e.getMessage());
			Utility.writeLog("RunID " + Utility.runID  + "Error !! Please check error message. "+ e.getMessage(), "Error", "", "ScriptRunner Startup", "db");
			//	System.exit(0);
			Utility.writeLog("RunID " + Utility.runID + " Application has ended.", "Info", "", "Application Ends", "DB"); 
			System.out.println("\nRunID " + Utility.runID + " Application has ended.");
		} catch (IOException e) {
			System.out.println("Error !! Please check error message "+ e.getMessage());
			Utility.writeLog("RunID " + Utility.runID  + "Error !! Please check error message. "+ e.getMessage(), "Error", "", "ScriptRunner Startup", "db");
			Utility.writeLog("RunID " + Utility.runID + " Application has ended.", "Info", "", "Application Ends", "DB"); 
			System.out.println("\nRunID " + Utility.runID + " Application has ended.");
		} catch (SQLException e) {
			System.out.println("Error !! Please check error message "+ e.getMessage());
			Utility.writeLog("RunID " + Utility.runID + " Error !! Please check error message. " + e.getMessage(),"Error","","ScriptRunner Startup","db");
			Utility.writeLog("RunID " + Utility.runID + " Application has ended.", "Info", "", "Application Ends", "DB"); 
			System.out.println("\nRunID " + Utility.runID + " Application has ended.");
		} catch (Exception e) {
			System.out.println("Error !! Please check error message "+ e.getMessage());
			Utility.writeLog("RunID " + Utility.runID + " Error !! Please check error message. " + e.getMessage(), "Error","","ScriptRunner Startup","db");
			Utility.writeLog("RunID " + Utility.runID + " Application has ended.", "Info", "", "Application Ends", "DB"); 
			System.out.println("\nRunID " + Utility.runID + " Application has ended.");
		}finally {
			if(con!=null)
				Utility.closeConnection(con);
		}

	}

	private static boolean proceedWithFactScriptExecution(String dbObject, Map<String, String> scriptExecuteMap ) {
		if(!Utility.isDimension(dbObject)) {
			return !scriptExecuteMap.containsKey("dimension");
		}
		return true;
	}

	private static void validateCMDLineArgs(String[] args) {
		if(args.length == 0 || (args.length !=1 && args.length !=2  && args.length !=3 && args.length !=4 ))
		{
			Utility.writeLog("Please provide valid arguments:\n 1. For NORMAL mode provide: <subsidirayId> \n 2. For RE-RUN mode provide: \n\"Re-Process\" <subsidirayId> <comma separated dimension and facts name> <RunID> \n 3. For RUN STATISTICS mode provide: \n\"RunStat\" <Optional : comma separated dimension and facts name>", "error","","Application StartUp","file");
			System.out.println("Please provide valid arguments:\n 1. For NORMAL mode provide: <subsidirayId> \n 2. For RE-RUN mode provide: \n Re-Process <subsidirayId> <comma separated dimension and facts name> <RunID> \n 3. For RUN STATISTICS mode provide: \n\"RunStat\" <Optional : comma separated dimension and facts name>");
			System.exit(0);
		}
	}

	private static void runStatistics(Connection con, DataLoader dl) throws ClassNotFoundException, SQLException {
		runStatisticsForTables(con, dl.getListOfDimensionsFacts());
	}

	private static void runStatisticsOnly(String[] args, Connection con) throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
		Utility.applicationStart(false,null);
		List<String> dimOrFactList = new ArrayList<String>();
		if(args.length>1) {
			String[] dimOrFactArr = args[1].split(",");
			dimOrFactList.addAll(Arrays.asList(dimOrFactArr));
		} else{
			
			Properties properties = new Properties();
			File pf = new File("config.properties");
			properties.load(new FileReader(pf));

			if (!properties.getProperty("Dimensions").isEmpty() && !properties.getProperty("Dimensions").equalsIgnoreCase("NONE")) {
				String[] dimensionsArr = properties.getProperty("Dimensions").split(",");
				dimOrFactList.addAll(Arrays.asList(dimensionsArr));
			}
			if (!properties.getProperty("Facts").isEmpty() && !properties.getProperty("Facts").equalsIgnoreCase("NONE")) {
				String[] factsArr = properties.getProperty("Facts").split(",");
				dimOrFactList.addAll(Arrays.asList(factsArr));
			}
		}
		
		runStatisticsForTables(con,dimOrFactList);

	}
	
	public static void runStatisticsForTables(Connection con, List<String> dimOrFactlist) throws ClassNotFoundException, SQLException {
		Class.forName(Utility.getConfig("RSCLASS"));
		if(con==null || con.isClosed()) {
			con = DriverManager.getConnection(Utility.getConfig("RSDBURL"),	Utility.getConfig("RSUID"), Utility.getConfig("RSPWD"));
		}
		Statement stmt = null;

		for(String dimOrFactTable :dimOrFactlist) {
			String sql="Vacuum dw_stage."+dimOrFactTable;
			stmt = con.createStatement();
			stmt.execute(sql);
			String sqlA="Analyze dw_stage."+dimOrFactTable;
			stmt = con.createStatement();
			stmt.execute(sqlA);

		}
		for(String dimOrFactTable :dimOrFactlist) {
			String sql="Vacuum dw."+dimOrFactTable;
			stmt = con.createStatement();
			stmt.execute(sql);
			String sqlA="Analyze dw."+dimOrFactTable;
			stmt = con.createStatement();
			stmt.execute(sqlA);

		}
		Utility.closeConnection(con);
		Utility.writeLog("RunID " + Utility.runID + " Vacuum, Analyzed Successfully.", "Info", "", "Run Statistics", "DB"); 
		System.out.println("\nRunID " + Utility.runID + " Vacuum, Analyzed Successfully.");
	}

}
