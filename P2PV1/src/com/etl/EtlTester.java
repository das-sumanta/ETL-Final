package com.etl;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

public class EtlTester {

	public static void main(String[] args) throws IOException, SQLException {

		Connection con = null;
		String aSQLScriptFilePath = "";
		List<String> dbObjects = new ArrayList<>();
		DataLoader dl=null;
		Properties properties = new Properties();
		File pf = new File("config.properties");
		properties.load(new FileReader(pf));
		String fileExtractURLTableName = properties.getProperty("FileExtractURLTableName");

		try {
			if (args.length == 0) {
				Utility.applicationStart(false);

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
								for (int i = 0; i < listOfFiles.length; i++) {

									tmp =  dbObject + "_prestage_opn.sql"; 
									long jobId = Utility.dbObjectJobIdMap.get(dbObject);
									if (tmp.equalsIgnoreCase(listOfFiles[i].getName())) {


										Class.forName(Utility.getConfig("RSCLASS"));
										con = DriverManager.getConnection(
												Utility.getConfig("RSDBURL"),
												Utility.getConfig("RSUID"),
												Utility.getConfig("RSPWD"));

										ScriptRunner scriptRunner = new ScriptRunner(con,false, true,dbObject);

										scriptRunner.runScript(new FileReader(
												aSQLScriptFilePath + File.separator
												+ listOfFiles[i].getName()),jobId);

										Utility.writeJobLog(jobId, "COMPLETED",
												dl.getSdf().format(Calendar
														.getInstance().getTime()));
										break;

									} else {
										continue;
									}
								}
							}

						}	
					}
				}
				Utility.writeLog("RunID " + Utility.runID + " Application has ended.", "info", "", "Application Ends", "DB"); 
				System.out.println("RunID " + Utility.runID + " Application has ended.");
			} else {

				System.out.println("DataLoader running in manual mode");
				String[] fileList = args[1].split(",");
				Utility.runID = Integer.valueOf(args[2]);
				Utility.applicationStart(true);

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
								for (int i = 0; i < listOfFiles.length; i++) {

									tmp =  dbObject + "_prestage_opn.sql"; 
									long jobId = Utility.dbObjectJobIdMap.get(dbObject);
									if (tmp.equalsIgnoreCase(listOfFiles[i].getName())) {

										Class.forName(Utility.getConfig("RSCLASS"));
										con = DriverManager.getConnection(
												Utility.getConfig("RSDBURL"),
												Utility.getConfig("RSUID"),
												Utility.getConfig("RSPWD"));

										ScriptRunner scriptRunner = new ScriptRunner(con, false, true, dbObject);

										scriptRunner.runScript(new FileReader(
												aSQLScriptFilePath + File.separator
												+ listOfFiles[i].getName()),jobId);

										Utility.writeJobLog(jobId, "COMPLETED",
												dl.getSdf().format(Calendar
														.getInstance().getTime()));
										break;

									} else {
										continue;
									}
								}
							}
						}	

					}
				}
				Utility.writeLog("RunID " + Utility.runID + " Application has ended.", "info", "", "Application Ends", "DB"); 
				System.out.println("RunID " + Utility.runID + " Application has ended.");
			}
		} catch (ClassNotFoundException e) {
			System.out.println("Error !! Please check error message "+ e.getMessage());
			Utility.writeLog("RunID " + Utility.runID  + "Error !! Please check error message. "+ e.getMessage(), "error", "", "ScriptRunner Startup", "db");
			System.exit(0);
		} catch (IOException e) {
			System.out.println("Error !! Please check error message "+ e.getMessage());
			Utility.writeLog("RunID " + Utility.runID  + "Error !! Please check error message. "+ e.getMessage(), "error", "", "ScriptRunner Startup", "db");

		} catch (SQLException e) {
			Utility.writeLog("RunID " + Utility.runID + " Error !! Please check error message. " + e.getMessage(),"error","","ScriptRunner Startup","db");
		} catch (Exception e) {
			Utility.writeLog("RunID " + Utility.runID + " Error !! Please check error message. " + e.getMessage(), "error","","ScriptRunner Startup","db");
		}

	}

}
