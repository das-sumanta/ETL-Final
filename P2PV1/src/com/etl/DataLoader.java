package com.etl;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

//import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.quote.AlwaysQuoteMode;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class DataLoader {
	private String appConfigPropFile;
	private List<String> dimensions;
	private List<String> facts;
	private Connection con;
	private String connectionString;
	private Statement statement;
	private ResultSet resultSet;
	private String query;
	private String extractLocationLocal;
	private String extractLocationURL;
	private String extractLocationS3;
	private String factsSqlScriptLocation;
	private TransferManager tx;
	private Upload upload;
	private String awsProfile;
	private String redShiftMasterUsername;
	private String redShiftMasterUserPassword;
	private String redShiftPreStageSchemaName;
	private String dbURL;
	private Map<String, String> checkList = new HashMap<String, String>();
	private char csvDelimiter;
	private String[] extractURL;
	private  List<String> extractURLTableName;
	private List<Long> insertIDList;
	private boolean isDWEntryRestricted;
	private String dateTimeFormat;
	private SimpleDateFormat sdf;
	private SimpleDateFormat sdfType1;

	public DataLoader() throws IOException, SQLException {
		
		
		appConfigPropFile = "config.properties";
		Properties properties = new Properties();
		File pf = new File(appConfigPropFile);
		properties.load(new FileReader(pf));

		if (!properties.getProperty("Dimensions").isEmpty() && !properties.getProperty("Dimensions").equalsIgnoreCase("NONE")) {
			String[] dimensionsArr = properties.getProperty("Dimensions").split(",");
			dimensions=new ArrayList<>(Arrays.asList(dimensionsArr));
		} else {
			System.out.println("No dimentions are mentioned in the config file. Facts processing cannot be done without dimension, hence terminating the program.");
			Utility.writeLog("Error !! No dimentions are mentioned in the config file. Facts processing cannot be done without dimension, hence terminating the program.",
					"Error","","Application Startup","db");
			System.exit(0);
		}

		if (!properties.getProperty("Facts").isEmpty() && !properties.getProperty("Facts").equalsIgnoreCase("NONE")) {
			String[] factsArr = properties.getProperty("Facts").split(",");
			facts=new ArrayList<>(Arrays.asList(factsArr));
		} else {
			facts = null;
		}

		extractLocationLocal = System.getProperty("user.dir") + File.separator
				+ "DB_Extracts" + File.separator +  Utility.getCurrentDate();
		extractLocationURL = System.getProperty("user.dir") + File.separator
				+ "URL_DB_Extracts" + File.separator +  Utility.getCurrentDate();
		extractLocationS3 = properties.getProperty("s3bucket");
		
		factsSqlScriptLocation = properties.getProperty("FactFileLoc");

		awsProfile = Utility.getConfig("AWSPROFILE");
		redShiftMasterUsername = Utility.getConfig("RSUID");;
		redShiftMasterUserPassword = Utility.getConfig("RSPWD");
		redShiftPreStageSchemaName = properties.getProperty("RSSCHEMAPRESTAGE");
		dbURL = Utility.getConfig("RSDBURL");
		csvDelimiter = properties.getProperty("CSVDelim").charAt(0);
		extractURL = properties.getProperty("FileExtractURL").split(",");
		dateTimeFormat = properties.getProperty("DateTimeFormat");
		sdf = new SimpleDateFormat(dateTimeFormat);
		sdfType1 =new SimpleDateFormat(properties.getProperty("DateTimeFormatType1"));
		
		if (!properties.getProperty("FileExtractURLTableName").isEmpty() && !properties.getProperty("FileExtractURLTableName").equalsIgnoreCase("NONE")) {
			String[] extractURLTableNameArr = properties.getProperty("FileExtractURLTableName").split(",");
			extractURLTableName=new ArrayList<>(Arrays.asList(extractURLTableNameArr));
		}

		
	//	DIM = properties.getProperty("Dimensions1").split(","); //TODO Remove In Utility

		try {
			new File(extractLocationLocal).mkdirs();
			new File(extractLocationURL).mkdirs();

			// Creating JDBC DB Connection
			Class.forName(Utility.getConfig("NETSUITEDRIVER"));

			connectionString = String
					.format("jdbc:ns://%s:%d;ServerDataSource=%s;encrypted=1;CustomProperties=(AccountID=%s;RoleID=%d)",
							Utility.getConfig("NETSUITESERVERHOST"), Integer.parseInt(Utility.getConfig("NETSUITEPORT")), Utility.getConfig("NETSUITEDATASOURCE"), Utility.getConfig("NETSUITEACCOUNTID"),
							Integer.parseInt(Utility.getConfig("NETSUITEROLEID")));

			con = DriverManager.getConnection(connectionString, Utility.getConfig("NETSUITELOGIN"), Utility.getConfig("NETSUITEPASSWORD"));

			Utility.writeLog("Application Started Successfully.RunID  of this session is "+ Utility.runID, "Info", "", "Application Startup", "db");

			System.out.println("************************************ WELCOME TO P2P DB DATA LOADER UTILITIES ************************************");

		} catch (ClassNotFoundException e) {
			System.out.println("Error !! Please check error message "
					+ e.getMessage());
			Utility.writeLog("RunID " + Utility.runID + "Error !! Please check error message. "
					+ e.getMessage(), "Error", "", "Application Startup", "db");
			System.exit(0);
		} 

	}

	public DataLoader(String mode, int runid) throws IOException,
			ClassNotFoundException, SQLException {
		appConfigPropFile = "config.properties";
		Properties properties = new Properties();
		File pf = new File(appConfigPropFile);
		properties.load(new FileReader(pf));

		extractLocationLocal = System.getProperty("user.dir") + File.separator
				+ "DB_Extracts" + File.separator + Utility.getCurrentDate();
		extractLocationS3 = properties.getProperty("s3bucket");
	
		factsSqlScriptLocation = properties.getProperty("FactFileLoc");


		awsProfile = Utility.getConfig("AWSPROFILE");
		redShiftMasterUsername = Utility.getConfig("RSUID");;
		redShiftMasterUserPassword = Utility.getConfig("RSPWD");
		redShiftPreStageSchemaName = properties.getProperty("RSSCHEMAPRESTAGE");
		dbURL = Utility.getConfig("RSDBURL");
		csvDelimiter = properties.getProperty("CSVDelim").charAt(0);
		extractURL = properties.getProperty("FileExtractURL").split(",");
		dateTimeFormat = properties.getProperty("DateTimeFormat");
		sdf = new SimpleDateFormat(dateTimeFormat);
		sdfType1 =new SimpleDateFormat(properties.getProperty("DateTimeFormatType1"));
		
		if (!properties.getProperty("FileExtractURLTableName").isEmpty() && !properties.getProperty("FileExtractURLTableName").equalsIgnoreCase("NONE")) {
			String[] extractURLTableNameArr = properties.getProperty("FileExtractURLTableName").split(",");
			extractURLTableName=new ArrayList<>(Arrays.asList(extractURLTableNameArr));
		}
		
		try {
			new File(extractLocationLocal).mkdirs();

			// Creating JDBC DB Connection
			Class.forName(Utility.getConfig("NETSUITEDRIVER"));
			
			connectionString = String
					.format("jdbc:ns://%s:%d;ServerDataSource=%s;encrypted=1;CustomProperties=(AccountID=%s;RoleID=%d)",
							Utility.getConfig("NETSUITESERVERHOST"), Integer.parseInt(Utility.getConfig("NETSUITEPORT")), Utility.getConfig("NETSUITEDATASOURCE"), Utility.getConfig("NETSUITEACCOUNTID"),
							Integer.parseInt(Utility.getConfig("NETSUITEROLEID")));

			con = DriverManager
					.getConnection(connectionString, Utility.getConfig("NETSUITELOGIN"), Utility.getConfig("NETSUITEPASSWORD"));


			Utility.writeLog(
					"Application Started Successfully.RunID  of this session is "
							+ Utility.runID, "Info", "", "Application Startup", "db");

			System.out
					.println("************************************ WELCOME TO P2P DB DATA LOADER UTILITIES ************************************");

		}  catch (ClassNotFoundException e) {
			System.out.println("Error !! Please check error message "+ e.getMessage());
			Utility.writeLog("RunID " + Utility.runID + "Error !! Please check error message. "
					+ e.getMessage(), "Error", "", "Application Startup", "db");
			System.exit(0);
		}

	}



	public void createDbExtract() throws IOException {
		createDBExtract(dimensions, facts, CommonConstants.RUNMODE_NORMAL, CommonConstants.DATAEXTRACTION);

	}

	public void createDBExtract(List<String> dimList, List<String> factList, String runMode,String process) throws IOException, FileNotFoundException {
		insertIDList = new ArrayList<>();
		String TBL_DEF_CONF = "tbl_def.properties";
		String factFileName;

		Properties properties = new Properties();
		File pf = new File(TBL_DEF_CONF);
		properties.load(new FileReader(pf));
		
		System.out.println("");

		for(String dimension : dimList) {
			System.out.println("\n--------------------------------------------------------");
			System.out.println("DataExtraction Operation Started for DB table :" + dimension);
			System.out.println("--------------------------------------------------------");

			Utility.writeLog("RunID " + Utility.runID+ " DataExtraction Operation Started for DB table "+ dimension, "Info", dimension,	process, "db");
			long jobId = Utility.writeJobLog(Utility.runID, dimension, runMode, "In-Progress");
			insertIDList.add(jobId);
			Utility.dbObjectJobIdMap.put(dimension,jobId);

			// LOAD USER SPECIFIC COLUMNS FROM THE TABLE DEFINED IN PROPERTIES FILE

			query = properties.getProperty(dimension);

			try {

				Utility.writeJobLog(jobId, "EXTRACTSTART", sdf.format(Calendar.getInstance().getTime()));

				Utility.writeLog("RunID " + Utility.runID + " Retrieving data for "	+ dimension, "Info", dimension, process, "db");

				System.out.println("Retrieving data...");

				statement = con.createStatement();

				System.out.println("Executing query for " + dimension+ " table...");

				resultSet = statement.executeQuery(query);

				Date curDate = new Date();
				String strDate = sdfType1.format(curDate);

				String fileName = extractLocationLocal + File.separator	+ dimension + "-" + "RunID-" + Utility.runID + "-" + strDate + ".csv";
				File file = new File(fileName);
				FileWriter fstream = new FileWriter(file);
				BufferedWriter out = new BufferedWriter(fstream);
				String str = "";
				List<String> columnNames = getColumnNames(resultSet);
				for (int c = 0; c < columnNames.size(); c++) {
					str = "\"" + columnNames.get(c) + "\"";
					out.append(str);
					if (c != columnNames.size() - 1) {
						out.append(csvDelimiter);
					}
				}

				// process results

				while (resultSet.next()) {

					List<Object> row = new ArrayList<Object>();

					row.add(Utility.runID);

					for (int k = 0; k < columnNames.size() - 1; k++) {
						row.add(resultSet.getObject(k + 1));

					}

					out.newLine();

					for (int j = 0; j < row.size(); j++) {
						if (!String.valueOf(row.get(j)).equals("null")) {
							String tmp = "\"" + String.valueOf(row.get(j)) + "\"";
							out.append(tmp);
							if (j != row.size() - 1) {
								out.append(csvDelimiter);
							}
						} else {
							if (j != row.size() - 1) {
								out.append(csvDelimiter);
							}
						}
					}
				}

				out.close();

				Utility.writeJobLog(jobId, "EXTRACTEND", sdf.format(Calendar.getInstance().getTime()));

				Utility.writeLog("RunID " + Utility.runID + " DataExtraction Operation for table " + dimension + " is extracted in " + fileName, "Info", dimension, process, "db");

				System.out.println("DataExtraction Operation for table " + dimension + " is extracted in " + fileName);

				checkList.put(dimension, "Extraction Done");

			} catch (SQLException e) {
				System.out.println("SQL Error!! Please Check the query and try again.\n"+ e.getMessage());
				checkList.put(dimension, "Extraction Error");

				Utility.writeLog("RunID " + Utility.runID + " SQL Error!!" + e.getMessage(), "Error", dimension, process, "db");

				Utility.writeJobLog(insertIDList.get(insertIDList.size()-1), "Error", "");

			} catch (IOException e) {

				System.out.println("IO Error!! Please check the error message.\n" + e.getMessage());
				checkList.put(dimension, "Extraction Error");

				Utility.writeLog("RunID " + Utility.runID + " Error!!" + e.getMessage(), "Error", dimension, process, "db");

				Utility.writeJobLog(insertIDList.get(insertIDList.size()-1), "Error", "");

			} catch (Exception e) {
				System.out.println("RunID " + Utility.runID+ " Error!! Please check the error message.\n"+ e.getMessage());
				checkList.put(dimension, "Extraction Error");

				Utility.writeLog("RunID " + Utility.runID + " Error!!" + e.getMessage(), "Error", dimension, process, "db");

				Utility.writeJobLog(insertIDList.get(insertIDList.size()-1), "Error", "");

			} //Removed finally continue  Not required will continue

		}

		/***************************** Fact Extraction Started **************************************/

		if (factList != null && !factList.isEmpty()) {

			Iterator<String> factItr = factList.iterator();
			while(factItr.hasNext()){
				String fact=factItr.next();
				System.out.println("\n--------------------------------------------------------");
				System.out.println("DataExtraction Operation Started for facts :" +fact);
				System.out.println("--------------------------------------------------------");
				Utility.writeLog("RunID " + Utility.runID+ " DataExtraction Operation Started for facts.", "Info", fact, process, "db");
				insertIDList.add(Utility.writeJobLog(Utility.runID, fact,runMode, "In-Progress"));
				Utility.dbObjectJobIdMap.put(fact,insertIDList.get(insertIDList.size() - 1));
				Utility.writeJobLog(insertIDList.get(insertIDList.size()-1), "EXTRACTSTART", sdf.format(Calendar.getInstance().getTime()));
				try {

					factFileName = factsSqlScriptLocation + File.separator+ fact + ".sql";
					createFactExtract(con, factFileName, fact, process, factItr);

				} catch (Exception e) {

					checkList.put(fact, "Extraction Error");
					System.out.println("RunID " + Utility.runID + " Error!! in extracting fact "+ fact +" due to "+ e.getMessage());

					Utility.writeLog("RunID " + Utility.runID + " Error!!" + e.getMessage(), "Error", fact, process, "db");
					Utility.writeJobLog(insertIDList.get(insertIDList.size()-1), "Error","");

				} //Removed finally continue  Not required will continue
			}
		} else {
			System.out.println("No facts are mentioned in the config file.");
		}
	}

	public void createDbExtract(char mode, String[] failedFileList)	throws IOException {
		List<String> dimList = new ArrayList<>();
		List<String> factList = new ArrayList<>();
		for(String file: failedFileList) {
			if(Utility.isDimension(file)) {
				dimList.add(file);
			} else {
				factList.add(file);
			}
		}
		dimensions = new ArrayList<>();
		facts = new ArrayList<>();
		dimensions.addAll(dimList); //Initializing Class level dimensions for future use
		facts.addAll(factList); //Initializing Class level facts for future use
		createDBExtract(dimensions, facts, CommonConstants.RUNMODE_REPROCESS, CommonConstants.DATAEXTRACTION_REPROCESS);

	}
	
	private void createFactExtract(Connection conn, String factFile,
			String factName, String process, Iterator<String> factItr) throws SQLException, IOException {

		Statement st = null;
		ResultSet rs = null;
		String lastModDate;
		String currDate = null;

		Properties tmpProp = new Properties();
		File tmpFile = new File("tmpFile.properties");
		tmpFile.createNewFile();

		try {
			tmpProp.load(new FileReader(tmpFile));

		} catch (IOException e) {

			throw new IOException(e.toString());
		}

		try {
			BufferedReader in = new BufferedReader(new FileReader(factFile));

			Scanner scn = new Scanner(in);
			scn.useDelimiter("/\\*[\\s\\S]*?\\*/|--[^\\r\\n]*|;");

			st = conn.createStatement();

			while (scn.hasNext()) {
				String line = scn.next().trim();

				if (!line.isEmpty()) {
					
					lastModDate = Utility.getDatesFromFact(factName, Utility.SUBID, Utility.factExtConstDt, Utility.factExtConstDt);
					String[] queryParts= line.split("UNION");
					StringBuilder finalQuery= new StringBuilder();
					for(String queryPart:queryParts){
						finalQuery.append(String.format(queryPart, Utility.SUBID,lastModDate));
						finalQuery.append("UNION");
					}
					finalQuery.delete(finalQuery.lastIndexOf("UNION"),finalQuery.length());
					Utility.writeLog("RunID " + Utility.runID + " Executing query for " + factName + " where DATE_LAST_MODIFIED >= " + lastModDate, "Info", factName, process, "DB");
					rs = st.executeQuery(finalQuery.toString());

					currDate = sdf.format(Calendar.getInstance().getTime());
					lastModDate =  Utility.SUBID + "," + lastModDate + "," + currDate;
					updateFactsProperty("tmpFile.properties", factName,	lastModDate);

					if (rs.next()) {

						System.out.println("Retrieving data...");

						Utility.writeLog("Retrieving data for " + factName, "Info",	factName, process, "DB");

						Date curDate = new Date();
						String strDate = sdfType1.format(curDate);

						String fileName = extractLocationLocal + File.separator
								+ factName + "-" + "RunID-" + Utility.runID + "-"
								+ strDate + ".csv";
						File file = new File(fileName);
						FileWriter fstream = new FileWriter(file);
						BufferedWriter out = new BufferedWriter(fstream);

						String str = "";
						List<String> columnNames = getColumnNames(rs);
						for (int c = 0; c < columnNames.size(); c++) {
							str = "\"" + columnNames.get(c) + "\"";
							out.append(str);
							if (c != columnNames.size() - 1) {
								out.append(csvDelimiter);
							}
						}

						while (rs.next()) {
							List<Object> row = new ArrayList<Object>();
							row.add(Utility.runID);
							for (int k = 0; k <columnNames.size() - 1; k++) {
								row.add(rs.getObject(k + 1)); 
							}

							out.newLine();
							for (int j = 0; j < row.size(); j++) {
								if (!String.valueOf(row.get(j)).equals("null")) {
									String tmp = "\""
											+ String.valueOf(row.get(j)) + "\"";
									out.append(tmp);
									if (j != row.size() - 1) {
										out.append(csvDelimiter);
									}
								} else {
									if (j != row.size() - 1) {
										out.append(csvDelimiter);
									}
								}
							}
						}
						out.close();
						fstream.close();

						checkList.put(factName, "Extraction Done");
						System.out.println("Extraction Completed for fact "+ factName);
						Utility.writeLog("RunID " + Utility.runID+ " Extraction Completed for fact " + factName, "Info", factName, process, "DB");
						Utility.writeJobLog(insertIDList.get(insertIDList.size()-1), "EXTRACTEND", sdf.format(Calendar.getInstance().getTime()));
						
					} else {
						factItr.remove();
						System.out.println("No resultset generated after executing query for "
										+ factName
										+ " where DATE_LAST_MODIFIED >= "
										+ lastModDate);
						Utility.writeLog("RunID "
										+ Utility.runID
										+ " No resultset generated after executing query for "
										+ factName
										+ " where DATE_LAST_MODIFIED >= "
										+ lastModDate, "Info", factName,
								process, "DB");
						Utility.writeJobLog(insertIDList.get(insertIDList.size()-1), "ERROR", null);
								
						checkList.put(factName, "Extraction Error");
						scn.close();

						if (st != null)
							st.close();
						return;

					}

				}

			}
			scn.close();
			if (st != null)
				st.close();

		} catch (FileNotFoundException e) {  //All commented as handled in calling method

		//	Utility.writeLog("RunID " + Utility.runID + " Error!!" + e.getMessage(), "Error", "", process, "DB"); 
			throw new FileNotFoundException(e.toString());

		} catch (SQLException e) {

		//	Utility.writeLog("RunID " + Utility.runID + " Error!!" + e.getMessage(), "Error", "", process, "DB");
			throw new SQLException(e.toString());

		} catch (IOException e) {

		//	Utility.writeLog("RunID " + Utility.runID + " Error!!" + e.getMessage(), "Error", "", process, "DB");
			throw new IOException(e.toString());

		} 

	}

	public void doAmazonS3FileTransfer() throws SecurityException, IOException {
		List<String> files = new ArrayList<>();
		if(extractURLTableName!=null && !extractURLTableName.isEmpty()) {
			files.addAll(dimensions);
			files.addAll(extractURLTableName);
			if(facts!=null && !facts.isEmpty()) {
				files.addAll(facts); 
			}
		} else {
			files.addAll(dimensions);
			if(facts!=null && !facts.isEmpty()) {
				files.addAll(facts);
			}
		}
 		
		s3AndRedShiftUpload(files, CommonConstants.S3_TRANSFER); 
    	deleteCurrentExtractedDir(new File(extractLocationLocal));

	}

	public void s3AndRedShiftUpload(List<String> files, String process) throws IOException {
		AWSCredentials credentials = null;
		try {

			credentials = new ProfileCredentialsProvider(awsProfile).getCredentials();
		} catch (Exception e) {
			System.out.println("Error in "+process+" Cannot load the credentials from the credential profiles file.");
			Utility.writeLog(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location, and is in valid format.", "Error", "",	process, "DB");
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location, and is in valid format.", e);

		}

		AmazonS3 s3 = new AmazonS3Client(credentials);
		Region usWest2 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usWest2);
		tx = new TransferManager(s3);

		String bucketName = extractLocationS3;
		
		System.out.println("\n--------------------------------------------------------");
		System.out.println("Uploading DB Extracts to Amazon S3");
		System.out.println("--------------------------------------------------------");

		for (int i = 0; i < files.size(); i++) {
			
			if (!checkErrorStatus("Extraction", files.get(i))) {
								
				Utility.writeJobLog(insertIDList.get(i), "S3LOADSTART", sdf.format(Calendar.getInstance().getTime()));

				try {

					File s3File = getLastFileModifiedFile(extractLocationLocal,	files.get(i));
					Utility.writeLog("RunID " + Utility.runID + " Uploading " + s3File.getName()+ " to Amazon S3 bucket " + bucketName,	"Info", files.get(i), process, "DB");
					System.out.println("Uploading " + s3File.getName()+ " to S3...");

					PutObjectRequest request = new PutObjectRequest(bucketName,
							 Utility.getCurrentDate() + "/" + s3File.getName(), s3File);
					upload = tx.upload(request);
					upload.waitForCompletion();
					if(upload.getState().ordinal()==2)
					{

						Utility.writeJobLog(insertIDList.get(i), "S3LOADEND", sdf.format(Calendar.getInstance().getTime()));

						Utility.writeLog("RunID " + Utility.runID +" "+ s3File.getName() + " transferred successfully.", "Info", files.get(i), process, "DB");

						System.out.println(files.get(i) + " file transferred.");

						Utility.writeJobLog(insertIDList.get(i), "REDSHIFTLOADSTART", sdf.format(Calendar.getInstance().getTime()));

						if(proceedWithFactsLoad(files.get(i),i))
							loadDataToRedShiftDB(files.get(i), s3File.getName(), i);

					}

				} catch (AmazonServiceException ase) {

					Utility.writeLog("Caught an AmazonServiceException, which means your request made it "
									+ "to Amazon S3, but was rejected with an error response for some reason."
									+ " Error Message:" + ase.getMessage()
									+ " HTTP Status Code: "
									+ ase.getStatusCode() + " AWS Error Code: "
									+ ase.getErrorCode() + " Error Type: "
									+ ase.getErrorType() + " Request Id: "
									+ ase.getRequestId(), "Error", "", process, "DB");
					System.out.println("Caught an AmazonServiceException, which means your request made it "
									+ "to Amazon S3, but was rejected with an error response for some reason.");
					System.out.println("Error Message:    " + ase.getMessage());
					System.out.println("HTTP Status Code: "	+ ase.getStatusCode());
					System.out.println("AWS Error Code:   "	+ ase.getErrorCode());
					System.out.println("Error Type:       " + ase.getErrorType());
					System.out.println("Request ID:       "	+ ase.getRequestId());

					Utility.writeJobLog(insertIDList.get(i), "Error","");

					checkList.put(files.get(i), "Loading Error");
					

				} catch (AmazonClientException ace) {
					checkList.put(files.get(i), "Loading Error");
					System.out.println("Error !! Please check error message "+ ace.getMessage());
					Utility.writeLog("Caught an AmazonClientException, which means the client encountered "
									+ "a serious internal problem while trying to communicate with S3, "
									+ "such as not being able to access the network."
									+ ace.getMessage(), "Error", files.get(i), process, "DB");
					Utility.writeJobLog(insertIDList.get(i), "Error","");
					
				} catch (Exception e) {
					checkList.put(files.get(i), "Loading Error");
					System.out.println("Error !! Please check error message "+ e.getMessage());
					Utility.writeLog("RunID " + Utility.runID + " " + e.getMessage(), "Error", files.get(i), process, "DB");
					Utility.writeJobLog(insertIDList.get(i), "Error","");
					
				} //Removed finally continue  Not required will continue

			} else {
				checkList.put(files.get(i), "Loading Error");  
				System.out.println("There is an issue while creating the extract of "+ files.get(i)	+ ". So loading operation is skipped for "+ files.get(i)+"\n");
				Utility.writeLog("As there is an issue while creating the extract of "+ files.get(i) + " so loading operation is skipped for "
						+ files.get(i), "Info", files.get(i), process, "DB");
				
			}
		}

		tx.shutdownNow();
	}

	
	private boolean proceedWithFactsLoad(String fact, int listIndex) throws IOException {
		boolean proceed = true;
		if(facts!=null && !facts.isEmpty()) {
			if(facts.contains(fact)) {
				for(String dimension : dimensions) {
					boolean dimLoadError = checkErrorStatus("Loading", dimension);
					if(dimLoadError) {
						proceed = false;
						System.out.println("There is an error in copying dimension "+dimension+" to Redshift, hence fact will not work properly, hence, dw entry will be restricted.\n");
						Utility.writeLog(
								"There is an error in copying dimension "+dimension
								+" to Redshift, hence fact will not work properly, hence, dw entry will be restricted.", 
								"Error", fact,"LoadRedshift", "DB");

						Utility.writeJobLog(insertIDList.get(listIndex), "Error","");
						isDWEntryRestricted=true;   
						break;
					}
				}
			}
		}
		return proceed;
	}	

	public void doAmazonS3FileTransfer(List<String> failedFileList) throws SecurityException, IOException {
	
		List<String> files = new ArrayList<>();
		if(extractURLTableName!=null && !extractURLTableName.isEmpty()) {
			files.addAll(dimensions);
			files.addAll(extractURLTableName);
			if(facts!=null && !facts.isEmpty()) {
				files.addAll(facts); 
			}
		} else {
			files.addAll(dimensions);
			if(facts!=null && !facts.isEmpty()) {
				files.addAll(facts);
			}
		}
		s3AndRedShiftUpload(files, CommonConstants.S3_TRANSFER_REPROCESS); 
	}

	public void loadDataToRedShiftDB(String tableName, String fileName, int listIndex) throws SecurityException, IOException {

		Connection conn = null;
		Statement stmt = null;
		AWSCredentials credentials = null;

		try {
			credentials = new ProfileCredentialsProvider(awsProfile)
					.getCredentials();
		} catch (Exception e) {
			System.out.println("Error in S3Transfer process : Cannot load the credentials from the credential profiles file");
			Utility.writeLog("Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location, and is in valid format.", "Error", "",	"S3Transfer", "DB");
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location, and is in valid format.", e);

		}

		AmazonS3 s3 = new AmazonS3Client(credentials);
		Region usWest2 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usWest2);

		String accKey = credentials.getAWSAccessKeyId();
		String scrtKey = credentials.getAWSSecretKey();

		System.out.println("RedShift Data loading started for " + tableName);
		Utility.writeLog("RedShift Data loading started for " + tableName, "Info", tableName, "LoadRedshift", "DB");
		try {

			// System.out.println("Waiting 1 min. for the availability of the file in Amazon S3");
			// Thread.sleep(60000);
			Class.forName(Utility.getConfig("RSCLASS"));

			// Open a connection and define properties.
			System.out.println("Connecting to Redshift Cluster...");

			Properties props = new Properties();

			props.setProperty("user", redShiftMasterUsername);
			props.setProperty("password", redShiftMasterUserPassword);
			conn = DriverManager.getConnection(dbURL, props);

			stmt = conn.createStatement();
			String sql;
			sql = "truncate table " + redShiftPreStageSchemaName + "."
					+ tableName;
			stmt.executeUpdate(sql);
			sql = "copy "
					+ redShiftPreStageSchemaName
					+ "."
					+ tableName
					+ " from 's3://"
					+ extractLocationS3
					+ "/"
					+  Utility.getCurrentDate()
					+ "/"
					+ fileName
					+ "' credentials 'aws_access_key_id="
					+ accKey
					+ ";aws_secret_access_key="
					+ scrtKey
					+ "' timeformat 'YYYY-MM-DD HH:MI:SS' escape removequotes delimiter as ',' IGNOREHEADER 1 ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF;";
			System.out.println("Executing Query...");
			Utility.writeLog("Executing Query...", "Info", tableName, "LoadRedshift", "DB");

			stmt.executeUpdate(sql);

			System.out.println("Done.\n");

			checkList.put(tableName, "Loading Done");
			Utility.writeLog("RedShift Data loading is completed successfully for "+ tableName, "Info", tableName, "LoadRedshift", "DB");
			
			Utility.writeJobLog(insertIDList.get(listIndex), "REDSHIFTLOADEND", sdf.format(Calendar.getInstance().getTime()));
			stmt.close();
			conn.close();

		} catch (Exception ex) {
			checkList.put(tableName, "Loading Error");
			if(Utility.isDimension(tableName)) {
				System.out.println("Error occured while loading data from S3 to Redshift Cluster for "+ tableName+ " " + ex.getMessage()+" hence, dw entry will be restricted.\n");

				Utility.writeLog("RunID "+ Utility.runID + " Error occured while loading data from S3 to Redshift Cluster for "
								+ tableName + " " + ex.getMessage() + " hence, dw entry will be restricted.", "Error", tableName, "LoadRedshift", "DB");
				
				Utility.writeJobLog(insertIDList.get(listIndex), "ERROR", sdf.format(Calendar.getInstance().getTime())); 
				isDWEntryRestricted=true; 
			} else {
				System.out.println("Error occured while loading data from S3 to Redshift Cluster for "+ tableName+ " " + ex.getMessage()+"\n");

				Utility.writeLog("RunID "+ Utility.runID + " Error occured while loading data from S3 to Redshift Cluster for "
								+ tableName + " " + ex.getMessage(), "Error", tableName, "LoadRedshift", "DB");
				
				Utility.writeJobLog(insertIDList.get(listIndex), "ERROR", sdf.format(Calendar.getInstance().getTime())); 
			//	isDWEntryRestricted=true; 
			}
			

		} finally {
			// Finally block to close resources.
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception ex) {
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}

	public void createDbExtractFromURL() throws IOException {

		ICsvMapReader mapReader = null;
		ICsvMapWriter mapWriter = null;
		FileOutputStream fos = null;
		Date curDate = new Date();
		String strDate = sdfType1.format(curDate);
		String dimName = "";
		
		try {
			int i = 0;
			for(String url :extractURL) {
				
				dimName = extractURLTableName.get(i);
				System.out.println("Fetching Flat file from URL " + url
						+ " and storing the file in " + extractLocationLocal
						+ " location");
				URL website = new URL(url);
				ReadableByteChannel rbc = Channels.newChannel(website.openStream());
				fos = new FileOutputStream(this.extractLocationLocal+ File.separator + extractURLTableName.get(i) +"_tmp_"+ strDate + ".csv");
				fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

				//CsvPreference prefs = CsvPreference.STANDARD_PREFERENCE;
				CsvPreference prefs = new CsvPreference.Builder('"',',',"\n").useQuoteMode(new AlwaysQuoteMode()).build();
				mapReader = new CsvMapReader(new FileReader(this.extractLocationLocal
						+ File.separator + extractURLTableName.get(i)+"_tmp_" + strDate + ".csv"),
						prefs);
				mapWriter = new CsvMapWriter(new FileWriter(this.extractLocationLocal
						+ File.separator + extractURLTableName.get(i) + "-" + "RunID-" + Utility.runID + "-"+ strDate + ".csv"),prefs);
				
				Utility.writeLog("Fetching Flat file from URL " + url
						+ " and storing the file in " + extractLocationLocal
						+ " location", "Info", extractURLTableName.get(i), "URLDataExtraction",	"DB");
				
				long jobId =Utility.writeJobLog(Utility.runID, extractURLTableName.get(i),"Normal", "In-Progress");
				insertIDList.add(jobId);
				
				Utility.writeJobLog(jobId, "EXTRACTSTART", sdf.format(Calendar.getInstance().getTime()));

				// header used to read the original file
				final String[] readHeader = mapReader.getHeader(true);

				// header used to write the new file
				// (same as 'readHeader', but with additional column)
				final String[] writeHeader = new String[readHeader.length + 1];
				System.arraycopy(readHeader, 0, writeHeader, 0, readHeader.length);
				final String timeHeader = "RunID";
				writeHeader[writeHeader.length - 1] = writeHeader[0];
				writeHeader[0] = timeHeader;

				mapWriter.writeHeader(writeHeader);

				Map<String, String> row;
				while ((row = mapReader.read(readHeader)) != null) {

					
					row.put(timeHeader, String.valueOf(Utility.runID));

					mapWriter.write(row, writeHeader);
				}
				
				mapReader.close();
				mapWriter.close();
				fos.close();
				File tmp = new File(extractLocationLocal+ File.separator + extractURLTableName.get(i) +"_tmp_"+ strDate + ".csv");
				
				tmp.delete();  
				Utility.writeJobLog(jobId, "EXTRACTEND", sdf.format(Calendar.getInstance().getTime()));
				checkList.put(extractURLTableName.get(i), "Extraction Done");
				i++;
			}
			

		} catch (IOException e) {
			System.out.println("Error !! Please check log for details");
			
			Utility.writeLog("Error \n " + e.getMessage(), "Error", dimName,
					"URLDataExtraction", "DB");
			
			Utility.writeJobLog(insertIDList.get(insertIDList.size()-1), "Error","");

		} catch(Exception ex) {

			Utility.writeLog("Error \n " + ex.getMessage(), "Error", dimName,
					"URLDataExtraction", "DB");
			
			Utility.writeJobLog(insertIDList.get(insertIDList.size()-1), "Error","");
		}
		
		finally {
			if (mapReader != null) {
				try {
					mapReader.close();
				} catch (IOException e) {
					System.out.println("Error !! Please check log for details");
					Utility.writeLog("Error \n " + e.getMessage(), "Error",
							dimName, "URLDataExtraction", "DB");
				}
			}
			if (mapWriter != null) {
				try {
					mapWriter.close();
				} catch (IOException e) {
					System.out.println("Error !! Please check log for details");
					Utility.writeLog("Error \n " + e.getMessage(), "Error",
							dimName, "URLDataExtraction", "DB");
				}
			}
			if (fos!=null)
				fos.close();
		}

	}

	public File getLastFileModifiedFile(String dir, String prefix) {
		File fl = new File(dir);
		File[] files = fl.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return file.isFile();
			}
		});
		long lastMod = Long.MIN_VALUE;
		File choice = null;
		for (File file : files) {
			if ((file.lastModified() > lastMod)
					&& (file.getName().startsWith(prefix))) {
				choice = file;
				lastMod = file.lastModified();
			}
		}
		return choice;
	}

	private List<String> getColumnNames(ResultSet resultSet)
			throws SQLException {
		List<String> columnNames = new ArrayList<String>();
		ResultSetMetaData metaData = resultSet.getMetaData();
		columnNames.add("RunID");
		for (int i = 0; i < metaData.getColumnCount(); i++) {
			// indexing starts from 1
			columnNames.add(metaData.getColumnName(i + 1));
		}
		return columnNames;
	}

	public List<String> getListOfDimensionsFacts() {
		List<String> files = new ArrayList<>();
		if (dimensions != null && !dimensions.isEmpty() && facts != null && !facts.isEmpty()) {
			files.addAll(dimensions);
			files.addAll(facts);
		} else if (dimensions != null && !dimensions.isEmpty()  && (facts == null || facts.isEmpty())) {
			files = dimensions;
		} else if (dimensions == null  || dimensions.isEmpty()  && (facts != null || !facts.isEmpty())) {
			files = facts;
		} 
		return files;
	}

	public void updateFactsProperty(String propName, String key, String value) throws FileNotFoundException {

		Properties props = new Properties();
		Writer writer = null;
		File f = new File(propName);
		if (f.exists()) {
			try {

				props.load(new FileReader(f));
				props.setProperty(key.trim(), value.trim());
				System.out.println("\nUpdated file "+propName+"\n");
				writer = new FileWriter(f);
				props.store(writer, null);
				writer.close();
			} catch (IOException e) {

				e.printStackTrace(); //TODO remove this
			}

		} else {
			throw new FileNotFoundException("Invalid Properties file or "+ propName + " not found");
		}
	}
	
	public boolean checkErrorStatus(String errorType, String tabName) {

		String status = checkList.get(tabName);
		
		boolean errorStatus = false;
		if(status != null ) {
			switch (errorType) {
			case "Extraction":
				if (status.equalsIgnoreCase("Extraction Error"))
					errorStatus = true;
				break;
	
			case "Loading":
				if (status.equalsIgnoreCase("Loading Error"))
					errorStatus = true;
				break;
			}
		}
		return errorStatus;

	}

	public boolean isDWEntryRestricted() {
		return isDWEntryRestricted;
	}

	public SimpleDateFormat getSdf() {
		return sdf;
	}

	public void deleteCurrentExtractedDir(File file) {
	    File[] contents = file.listFiles();
	    if (contents != null) {
	        for (File f : contents) {
	        	deleteCurrentExtractedDir(f);
	        }
	    }
	    file.delete();
	}
	
}
