package com.etl;


import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class RelationerDataLoaderForEmployees {

	private String appConfigPropFile;
	private String msUid;
	private String msPwd;
	private String conStrMS;
	private Connection msCon;
	private String baseTbl;
	private String refTbl;
	private String finalTbl;
	private int hierarchyDepth;
	private String redShiftSchemaNamePreStage;
	private String redShiftSchemaNameStage;
	private String redShiftSchemaNameFinal;
	
	public RelationerDataLoaderForEmployees() {

		appConfigPropFile = "config.properties";
		Properties properties = new Properties();
		File pf = new File(appConfigPropFile);
		try {
			properties.load(new FileReader(pf));

		} catch (IOException e) {

			System.out.println("Error loading properties file.\n"
					+ e.getMessage());
		}

		msUid = Utility.getConfig("RSUID");
		msPwd = Utility.getConfig("RSPWD");
		conStrMS = Utility.getConfig("RSDBURL");
		baseTbl = properties.getProperty("TBLBaseEmp");
		refTbl = properties.getProperty("TBLRefEmp");
		finalTbl = properties.getProperty("TBLFinalEmp");
		redShiftSchemaNamePreStage = properties.getProperty("RSSCHEMAPRESTAGE");
		redShiftSchemaNameStage = properties.getProperty("RSSCHEMASTAGE");
		redShiftSchemaNameFinal = properties.getProperty("RSSCHEMA");
		
			
		try {
			Class.forName("com.amazon.redshift.jdbc41.Driver");
			Properties props = new Properties();

			props.setProperty("user", msUid);
			props.setProperty("password", msPwd);
			msCon = DriverManager.getConnection(conStrMS, props);
			
			createAppTmpTbl();
			populateAppTmpTbl();
			
		} catch (ClassNotFoundException e) {
			
			System.out.println("Error!! " + e.getMessage());
			Utility.writeLog("Error!!! " + e.getMessage(), "Error", "Employee", "Employee_Hierarchy_Startup", "db");
			
		} catch (SQLException e) {
			
			System.out.println("Error!! " + e.getMessage());
			Utility.writeLog("Error!!! " + e.getMessage(), "Error", "Employee", "Employee_Hierarchy_Startup", "db");
		}

	}

	public void processResult() throws SQLException {
		String sql = "";
		PreparedStatement ps = null;
		int lvl = 0, tot  = 0; 
		System.out.println("Inserting top level employees to the organization database..");
		Utility.writeLog("Inserting top level employees to the organization database..", "Info", baseTbl, "Top Level Employee Insertion", "db");
			
		try {
												
				sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
						+ baseTbl + "(employee_id, employee_name, employee_job_title, "
						+ "supervisor_id, supervisor_name, supervisor_job_title,level) "
						+ "SELECT employee_id, employee_name, job_title, supervisor_id, supervisor_name, supervisor_job_title, ?"
						+ " FROM "+ redShiftSchemaNamePreStage + "." + refTbl
					+ " WHERE (SUPERVISOR_ID is null)";
				ps = msCon.prepareStatement(sql);
				ps.setInt(1, 0);
				tot = ps.executeUpdate();
				System.out.println("No of employess added to this level are " + tot);
				Utility.writeLog("No of employess added to this level are " + tot, "Info", baseTbl, "Top Level Employee Insertion", "db");
				
				
				
				addEmployeeBasedOnLavel(lvl + 1, lvl);
				addEmployeeBasedOnQLavel();
				
				msCon.commit();
				
				System.out.println("All employees are added to the database. Program will now exit.");
				Utility.writeLog("All employees are added to the database. Program will now exit.", "Info", finalTbl, "Employee_Hierarchy_End", "db");

			} catch (SQLException e) {
				
				System.out.println("Error!!\n" + e.getMessage());
				Utility.writeLog("Error !!" + e.getMessage(), "Error", baseTbl, "Employee Insertion", "db");
				msCon.rollback();
				
			} finally {
				
				try {
					if (ps != null)
						ps.close();
				} catch (Exception ex) {
				}
				try {
					if (msCon != null)
						msCon.close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

	}

	public void addEmployeeBasedOnLavel(int nxtLvl, int prvLvl) throws SQLException {

		String sql = "";
		PreparedStatement ps = null;
		int res = 0, lvl = prvLvl;
				
		System.out.println("Inserting employees of level " + nxtLvl + " to the organization database..");
		Utility.writeLog("Inserting employees of level " + nxtLvl + " to the organization database..", "Info", baseTbl, "Level " + nxtLvl + " Employee Insertion", "db");
		
		sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
				+ baseTbl
				+ " (employee_id,employee_name,employee_job_title,supervisor_id,supervisor_name,supervisor_job_title,level) "
				+ "SELECT b.employee_id,b.employee_name,b.job_title,b.supervisor_id,b.supervisor_name,b.supervisor_job_title,? FROM "
				+ redShiftSchemaNamePreStage + "." 
				+ baseTbl + " a," + redShiftSchemaNamePreStage + "." + refTbl + " b WHERE a.LEVEL = ? and a.EMPLOYEE_ID = b.SUPERVISOR_ID";

		try {

			
			ps = msCon.prepareStatement(sql);
			ps.setInt(1, nxtLvl);
			ps.setInt(2, prvLvl);
			res = ps.executeUpdate();
			
			System.out.println("No of employess added to the level:- " + nxtLvl + " are " + res);
			Utility.writeLog("No of employess added to the level:- " + nxtLvl + " are " + res, "Info", baseTbl, "Top Level Employee Insertion", "db");

			if (res > 0) {
				
				lvl++;
				nxtLvl = lvl + 1;
				prvLvl = lvl;
				addEmployeeBasedOnLavel(nxtLvl, prvLvl);
				

			} else {
				hierarchyDepth = prvLvl;
				return;
			}

		} catch (SQLException e) {

			System.out.println("Error!!" + System.getProperty("line.separator")
					+ e.getMessage());
			
			Utility.writeLog("Error!! " + e.getMessage(), "Error", baseTbl, "Level " + nxtLvl + " Employee Insertion", "db");
			
			msCon.rollback();
			
		}

	}
	
	public void addEmployeeBasedOnQLavel() throws SQLException {

		String sql = "";
		PreparedStatement ps = null;
		int res = 0;
		
					
		try {
			
			
			//LOOP THROUGH THE DEPTH OF THE EMPLOYEE-BOSS HIERERCHY 
			
			for(int i = 1; i <= hierarchyDepth; i++) {
				
				if(i == 1){
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (employee_id,employee_name,employee_job_title,"
							+ "supervisor_id,supervisor_name,supervisor_job_title,level,q_level) "
							+ "SELECT employee_id, employee_name, employee_job_title,supervisor_id,supervisor_name,supervisor_job_title, ?, ? "
							+ " FROM " + redShiftSchemaNamePreStage + "." + baseTbl + " WHERE LEVEL = ?";
													
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, i);
					ps.setInt(2, i-1);
					ps.setInt(3, i);
					res = ps.executeUpdate();
					System.out.println("No. of employess added to the level " + i + " are " + res);
					Utility.writeLog("No. of employess added to the level " + i + " are " + res, "Info", finalTbl, "Level " + i + " Employee Insertion", "db");
					
				} else {
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (employee_id,employee_name,employee_job_title,"
							+ "supervisor_id,supervisor_name,supervisor_job_title,level,q_level) "
							+ "SELECT employee_id, employee_name, employee_job_title,supervisor_id,supervisor_name,supervisor_job_title, ?, ? "
							+ " FROM " + redShiftSchemaNamePreStage + "." + baseTbl + " WHERE LEVEL = ?";
													
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, 1);
					ps.setInt(2, i);
					ps.setInt(3, i);
					res = ps.executeUpdate();
					System.out.println("No. of employess added to the level " + i +" are " + res);
					Utility.writeLog("No. of employess added to the level " + i + " are " + res, "Info", finalTbl, "Level " + i + " Employee Insertion", "db");
				}
				
				for(int j = 1; j < i; j++){
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (employee_id,employee_name,employee_job_title,"
							+ "supervisor_id,supervisor_name,supervisor_job_title,level,q_level) "
							+ " SELECT b.employee_id, b.employee_name,b.employee_job_title,a.supervisor_id,a.supervisor_name,a.supervisor_job_title, ? , ? FROM "
							+ "(SELECT employee_id,employee_name,employee_job_title,supervisor_id,supervisor_name,supervisor_job_title,level FROM "
							+ redShiftSchemaNamePreStage + "." + baseTbl + " WHERE level = ?) a, "
							+ "(SELECT employee_id,employee_name,employee_job_title,supervisor_id,supervisor_name,supervisor_job_title,level,q_level"
							+ " FROM "+ redShiftSchemaNamePreStage + "." + finalTbl + " WHERE Q_LEVEL = ? and LEVEL = ?) b "
							+ "WHERE a.EMPLOYEE_ID = b.SUPERVISOR_ID";
					
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, j+1);
					ps.setInt(2, i);
					ps.setInt(3, i-j);
					ps.setInt(4, i);
					ps.setInt(5, j);
					res = ps.executeUpdate();
					System.out.println("No. of employess added to this level are " + res);
					Utility.writeLog("No. of employess added to this level are " + res, "Info", finalTbl, "Level " + j+1 + " Employee Insertion", "db");

				}
			}
			sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (employee_id,employee_name,employee_job_title,supervisor_id,supervisor_name,supervisor_job_title,level) "
					+ "SELECT DISTINCT employee_id , employee_name, employee_job_title,employee_id , employee_name, employee_job_title, 0 FROM " + redShiftSchemaNamePreStage + "." + baseTbl;
			
			ps = msCon.prepareStatement(sql);
			res = ps.executeUpdate();
			System.out.println("No. of employess added to this level are " + res);
			Utility.writeLog("No. of employess added to this level are " + res, "Info", finalTbl, "Final Level Employee Insertion", "db");


		} catch (SQLException e) {

			System.out.println("Error!!" + System.getProperty("line.separator")
					+ e.getMessage());
			
			msCon.rollback();
			
			Utility.writeLog("Error!!" + e.getMessage(), "Error", finalTbl, "Employee Insertion", "db");
		}

	}
	
	public void createAppTmpTbl() {
		
		Statement stmt = null;
		String sql;
		System.out.println("Temporary Table Creation is started...");
		
		Utility.writeLog("Temporary Table Creation is started..." , "Info", "Employee", "Employee_Hierarchy_Startup", "db");
		try {
			stmt = msCon.createStatement();
			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + refTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creating Table " + refTbl + "...");
			Utility.writeLog("Creating Table " + refTbl + "..." , "Info", "Employee", "Employee_Hierarchy_Startup", "db");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + refTbl + "(employee_id integer,"
					+ "employee_name varchar(500), job_title varchar(500), supervisor_id integer, "
					+ "supervisor_name varchar(500), supervisor_job_title varchar(500));";
			stmt.execute(sql);
			System.out.println("Done..");
			

			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + baseTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creating Table " + baseTbl + "...");
			Utility.writeLog("Creating Table " + baseTbl + "..." , "Info", "Employee", "Employee_Hierarchy_Startup", "db");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + baseTbl + "(employee_id integer,"
					+ "employee_name varchar(500), employee_job_title varchar(500), supervisor_id integer, "
					+ "supervisor_name varchar(500), supervisor_job_title varchar(500), level integer);";
			stmt.execute(sql);
			System.out.println("Done..");
			
			
			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + finalTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creating Table " + finalTbl + "...");
			Utility.writeLog("Creating Table " + finalTbl + "..." , "Info", "Employee", "Employee_Hierarchy_Startup", "db");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + finalTbl + "(employee_id integer,"
					+ "employee_name varchar(500), employee_job_title varchar(500), supervisor_id integer, "
					+ "supervisor_name varchar(500), supervisor_job_title varchar(500), level integer, q_level integer);";
			stmt.execute(sql);
			System.out.println("Done..");
			
			
		} catch (SQLException e) {
			
			System.out.println("Error!! " + e.getMessage());
			Utility.writeLog("Error!! " + e.getMessage(), "Error", "Employee", "Employee_Hierarchy_Startup", "db");
		}
		
	}
	
	public void populateAppTmpTbl() {
		
		Statement stmt = null;
		String sql;
		System.out.println("Populating table " + refTbl + " with the data of employees table");
		Utility.writeLog("Populating table " + refTbl + " with the data of employees table", "Info", "Employee", "Employee_Hierarchy_Startup", "db");
		try {
			stmt = msCon.createStatement();
			
			sql = "INSERT INTO " + redShiftSchemaNamePreStage + "." + refTbl 
					+ " SELECT a.employee_id , a.full_name as employee_name , a.jobtitle, "
					+ "a.supervisor_id , b.full_name as supervisor_name, b.jobtitle"
					+ " FROM "+ redShiftSchemaNamePreStage + ".employees a, "+ redShiftSchemaNamePreStage + ".employees b "
					+ " WHERE a.supervisor_id = b.employee_id(+);";
			
			int res = stmt.executeUpdate(sql);
			System.out.println(res + " rows are populated.");
			Utility.writeLog(res + " rows are populated.", "Info", "Employee", "Employee_Hierarchy_Startup", "db");
			
		} catch(SQLException e) {
			System.out.println("Error!! " + e.getMessage());
			Utility.writeLog("Error !!", "Error", "Employee", "Employee_Hierarchy_Startup", "db");
		}
		
		
	}
	

}
