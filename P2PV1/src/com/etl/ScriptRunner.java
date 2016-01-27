package com.etl;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;



public class ScriptRunner {

    public static final String DEFAULT_DELIMITER = ";";
    public static final String PL_SQL_BLOCK_SPLIT_DELIMITER = "+";
    public static final String PL_SQL_BLOCK_END_DELIMITER = "#";

    private final boolean autoCommit, stopOnError;
    private final Connection connection;
    private String delimiter = ScriptRunner.DEFAULT_DELIMITER;
    private final PrintWriter out, err;
    private String stageDesc;
    private String entity;
    Map<String,String> scriptExecuteMap = new HashMap<>();
   
    public ScriptRunner(final Connection connection, final boolean autoCommit, final boolean stopOnError,String entity, Map<String, String> scriptExecuteMap) {
        if (connection == null) {
            throw new RuntimeException("ScriptRunner requires an SQL Connection");
        }
		
        this.connection = connection;
        this.autoCommit = autoCommit;
        this.stopOnError = stopOnError;
        this.out = new PrintWriter(System.out);
        this.err = new PrintWriter(System.err);
        this.entity = entity;
        stageDesc = "";
        this.scriptExecuteMap.putAll(scriptExecuteMap);
        

    }

    public void runScript(final Reader reader, long jobId, Map<String, String> scriptExecuteMap) throws SQLException, IOException {
        final boolean originalAutoCommit = this.connection.getAutoCommit();
        try {
            if (originalAutoCommit != this.autoCommit) {
                this.connection.setAutoCommit(this.autoCommit);
            }
            this.runScript(this.connection, reader, jobId, scriptExecuteMap);
        } finally {
            this.connection.setAutoCommit(originalAutoCommit);
        }
    }

    private void runScript(final Connection conn, final Reader reader, long jobId, Map<String, String> scriptExecuteMap) throws SQLException, IOException {
        StringBuffer command = null;
       
        Table table = null;
       
        try {
            final LineNumberReader lineReader = new LineNumberReader(reader);
            String line = null;
            while ((line = lineReader.readLine()) != null) {
                if (command == null) {
                    command = new StringBuffer();
                }

                if (table == null) {
                    table = new Table();
                }

                String trimmedLine = line.trim();
                
                              
               if (trimmedLine.startsWith("/*") && trimmedLine.endsWith("*/")) {
            	   
            	   stageDesc = trimmedLine.substring(trimmedLine.indexOf("*") + 1, trimmedLine.lastIndexOf("*")).trim();
            	   
            	          	   
               }
               
				// Interpret SQL Comment & Some statement that are not executable
                if (trimmedLine.isEmpty() || trimmedLine.startsWith("--")
                        || trimmedLine.startsWith("//")
                        || trimmedLine.startsWith("#")
                        || trimmedLine.startsWith("/*")
                        || trimmedLine.toLowerCase().startsWith("rem inserting into")
                        || trimmedLine.toLowerCase().startsWith("set define off")) {

                    // do nothing...
                } else if (trimmedLine.endsWith(this.delimiter) || trimmedLine.endsWith(PL_SQL_BLOCK_END_DELIMITER)) { // Line is end of statement
                    
                    // Append
                    if (trimmedLine.endsWith(this.delimiter)) {
                        command.append(line.substring(0, line.lastIndexOf(this.delimiter)));
                        command.append(" ");
 
                    } else if (trimmedLine.endsWith(PL_SQL_BLOCK_END_DELIMITER)) {
                        command.append(line.substring(0, line.lastIndexOf(PL_SQL_BLOCK_END_DELIMITER)));
                        command.append(" ");
                        
                    }

                    Statement stmt = null;
                    ResultSet rs = null;
                    try {
                        stmt = conn.createStatement();
                        boolean hasResults = false;
                        if (this.stopOnError) { 
                        	System.out.println("Executing query ->" + command.toString());
                            hasResults = stmt.execute(command.toString());
                            //writeDBLog(runID,stmt.getUpdateCount() + " row(s) affected.",entity, this.errorCode,"Info" );
           //                 System.out.println(stmt.getUpdateCount()+"--this.stageDesc="+this.stageDesc);
           //                 Utility.writeLog(stmt.getUpdateCount() + " row(s) affected.", "Info", entity, this.stageDesc, "DB"); //Added Jan18
                        } else {
                            try {
                            	System.out.println("Executing query ->" + command.toString());
                            	stmt.execute(command.toString());
                            	Utility.writeLog(stmt.getUpdateCount() + " row(s) affected.", "Info", entity, this.stageDesc, "DB");
                            	
                            } catch (final SQLException e) {
                                e.fillInStackTrace();
                                err.println("Error executing SQL Command: \"" + command + "\"");
                                err.println(e);
                                err.flush();
                                
                                Utility.writeLog("RunID " + Utility.runID + " Error!!" + e.getMessage(), "error", entity, this.stageDesc, "DB");
                                throw e;
                            }
                        }

                        rs = stmt.getResultSet();
                        if (hasResults && rs != null) {
                            // Print & Store result column names
                            final ResultSetMetaData md = rs.getMetaData();
                            final int cols = md.getColumnCount();
                           
                            int recordsIdentified=0;
                            while (rs.next()) {
                                for (int i = 1; i <= cols; i++) {
                                    final String value = rs.getString(i);
                                    recordsIdentified=Integer.valueOf(value);
                                }
                                
                            }
                            out.flush();
                            System.out.println(this.stageDesc+"="+recordsIdentified);
                            Utility.writeLog(recordsIdentified + " row(s) affected.", "Info", entity, this.stageDesc, "DB"); //Added Jan18

                        } else {
                            out.println(stmt.getUpdateCount() + " row(s) affected.");
                            out.flush();
                            if(!stageDesc.equals("")) {
                            	Utility.writeLog(stmt.getUpdateCount() + " row(s) affected.", "Info", entity, this.stageDesc, "DB");
                                stageDesc = "";
                            }
                            
                        }
                        command = null;
                                        	
                    } finally {
                    	
                        if (rs != null) {
                            try {
                                rs.close();
                            } catch (final Exception e) {
                                err.println("Failed to close result: " + e.getMessage());
                                err.flush();
                            }
                        }
                        if (stmt != null) {
                            try {
                                stmt.close();
                                
                            } catch (final Exception e) {
                                err.println("Failed to close statement: " + e.getMessage());
                                err.flush();
                            }
                        }
                    }
                } else if (trimmedLine.endsWith(PL_SQL_BLOCK_SPLIT_DELIMITER)) {
                    command.append(line.substring(0, line.lastIndexOf(PL_SQL_BLOCK_SPLIT_DELIMITER)));
                    command.append(" ");
                } else { // Line is middle of a statement

                    // Append
                    command.append(line);
                    command.append(" ");
                    
                }
            }
            if (!this.autoCommit) { 
                conn.commit();
            }
            
        } catch (SQLException e) {
            conn.rollback();
            populateScriptExecuteMap(scriptExecuteMap, entity,"Error");
            System.out.println("Error executing SQL Command: \"" + command + "\"");
            Utility.writeLog("RunID " + Utility.runID + " Error!!" + e.getMessage(), "Error", entity, this.stageDesc, "DB");
            Utility.writeJobLog(jobId, "Error","");
            
            
        } catch (IOException e) {
            System.out.println("Error reading in SQL files..");
            populateScriptExecuteMap(scriptExecuteMap, entity,"Error");
            Utility.writeLog("RunID " + Utility.runID + " Error!!" + e.getMessage(), "Error", entity, this.stageDesc, "DB");
            Utility.writeJobLog(jobId, "Error","");
            
        } catch (Exception e) {
        	populateScriptExecuteMap(scriptExecuteMap, entity,"Error");
        	System.out.println("Error!!!! " + e.getMessage());
            Utility.writeLog("RunID " + Utility.runID + " Error!!" + e.getMessage(), "Error", entity, this.stageDesc, "DB");
            Utility.writeJobLog(jobId, "Error","");
        }
    }
    
    public void populateScriptExecuteMap(Map<String,String> scriptExecuteMap, String dimOrFact, String output) {
		if(Utility.isDimension(dimOrFact)) {
			scriptExecuteMap.put("dimension",output);
		} else {
			scriptExecuteMap.put(dimOrFact,output);
		}
	}
}
