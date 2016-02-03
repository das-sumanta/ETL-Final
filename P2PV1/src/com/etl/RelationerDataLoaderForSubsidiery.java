package com.etl;


import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class RelationerDataLoaderForSubsidiery {

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
	
	public RelationerDataLoaderForSubsidiery() {

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
		baseTbl = properties.getProperty("TBLBaseSub");
		refTbl = properties.getProperty("TBLRefSub");
		finalTbl = properties.getProperty("TBLFinalSub");
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
			Utility.writeLog("Error!!! " + e.getMessage(), "Error", "Subsidiery", "Subsidiery_Hierarchy_Startup", "db");
			
		} catch (SQLException e) {
			
			System.out.println("Error!! " + e.getMessage());
			Utility.writeLog("Error!!! " + e.getMessage(), "Error", "Subsidiery", "Subsidiery_Hierarchy_Startup", "db");
		}

	}

	public void processResult() throws SQLException {
		String sql = "";
		PreparedStatement ps = null;
		int lvl = 0, tot  = 0; 
		System.out.println("Inserting top level subsidieries to the organization database..");
		Utility.writeLog("Inserting top level subsidieries to the organization database...", "Info", baseTbl, "Top Level Location Insertion", "db");
			try {
												
				sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
						+ baseTbl + "(subsidiary_id, full_name,"
						+ "parent_id, parent_full_name) "
						+ "SELECT subsidiary_id, full_name, parent_id, parent_full_name"
						+ " FROM "+ redShiftSchemaNamePreStage + "." + refTbl
					+ " WHERE (parent_id is null)";
				
				ps = msCon.prepareStatement(sql);
				//ps.setInt(1, null);
				tot = ps.executeUpdate();
				System.out.println("No. of subsidieries added to this level are " + tot); 
				Utility.writeLog("No of subsidieries added to this level are " + tot, "Info", baseTbl, "Top Level subsidieries Insertion", "db");
				
				addSubsidieryBasedOnLavel(lvl + 1, lvl); 
				addSubsiBasedOnQLavel();
				
				System.out.println("All subsidieries are added to the database. Program will now exit.");
				Utility.writeLog("No of subsidieries added to this level are " + tot, "Info", baseTbl, "Top Level Location Insertion", "db");
				msCon.close();

			} catch (SQLException e) {
				
				System.out.println("Error!!\n" + e.getMessage());
				Utility.writeLog("Error !!" + e.getMessage(), "Error", "Subsidiery", "Subsidiery Insertion", "db");
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

	public void addSubsidieryBasedOnLavel(int nxtLvl, int prvLvl) throws SQLException {

		String sql = "";
		PreparedStatement ps = null;
		int res = 0, lvl = prvLvl;
		
		
		 
		System.out.println("Inserting next level subsidieries " + nxtLvl + " to the organization database..");
		Utility.writeLog("Inserting subsidieries of level " + nxtLvl + " to the organization database..", "Info", baseTbl, "Level " + nxtLvl + " Subsidiery Insertion", "db");
		
		if(prvLvl == 0) {
		sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
				+ baseTbl
				+ " (subsidiary_id, full_name, parent_id, parent_full_name,level) "
				+ "SELECT b.subsidiary_id, b.full_name, b.parent_id, b.parent_full_name,1 FROM "
				+ redShiftSchemaNamePreStage + "." 
				+ baseTbl + " a," + redShiftSchemaNamePreStage + "." + refTbl + " b WHERE a.LEVEL is null and a.subsidiary_id = b.parent_id";
		
		ps = msCon.prepareStatement(sql);
		res = ps.executeUpdate();
		
		
		} else {
			
			sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
					+ baseTbl
					+ " (subsidiary_id, full_name, parent_id, parent_full_name,level) "
					+ "SELECT b.subsidiary_id, b.full_name, b.parent_id, b.parent_full_name,? FROM "
					+ redShiftSchemaNamePreStage + "." 
					+ baseTbl + " a," + redShiftSchemaNamePreStage + "." + refTbl + " b WHERE a.LEVEL = ? and a.subsidiary_id = b.parent_id";
			
			ps = msCon.prepareStatement(sql);
			ps.setInt(1, nxtLvl);
			ps.setInt(2, prvLvl);
			res = ps.executeUpdate();
			
		}

		try {

						
			System.out.println("No of subsidieries added to the level:- " + nxtLvl + " are " + res);
			Utility.writeLog("No of subsidieries added to the level:- " + nxtLvl + " are " + res, "Info", baseTbl, "Level " + nxtLvl + "Subsidiery Insertion", "db");

			if (res > 0) {
				lvl++;
				nxtLvl = lvl + 1;
				prvLvl = lvl;
				addSubsidieryBasedOnLavel(nxtLvl, prvLvl);
				

			} else {
				hierarchyDepth = prvLvl;
				return;
			}

		} catch (SQLException e) {

			System.out.println("Error!!" + System.getProperty("line.separator")
					+ e.getMessage());
			
			msCon.rollback();
			
			Utility.writeLog("Error!! " + e.getMessage(), "Error", baseTbl, "Level " + nxtLvl + "Subsidiery Insertion", "db");
		}

	}
	
	public void addSubsiBasedOnQLavel() throws SQLException {

		String sql = "";
		PreparedStatement ps = null;
		int res = 0;
		
					
		try {

			for(int i = 1; i <= hierarchyDepth; i++) {
				
				if(i == 1){
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (subsidiary_id, full_name, parent_id, parent_full_name,level,q_level) "
							+ "SELECT subsidiary_id, full_name, parent_id, parent_full_name, ?, ? "
							+ " FROM " + redShiftSchemaNamePreStage + "." + baseTbl + " WHERE LEVEL = ?";
													
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, i);
					ps.setInt(2, i-1);
					ps.setInt(3, i);
					res = ps.executeUpdate();
					System.out.println("No. of subsidieries added to the level " + i + " are " + res);
					Utility.writeLog("No. of subsidieries added to the level " + i + " are " + res, "Info", finalTbl, "Level " + i + "Subsidiery Insertion", "db");
				} else {
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (subsidiary_id, full_name, parent_id, parent_full_name,level,q_level) "
							+ "SELECT subsidiary_id,full_name, parent_id, parent_full_name, ?, ? "
							+ " FROM " + redShiftSchemaNamePreStage + "." + baseTbl + " WHERE LEVEL = ?";
													
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, 1);
					ps.setInt(2, i);
					ps.setInt(3, i);
					res = ps.executeUpdate();
					System.out.println("No. of subsidieries added to the level " + i +" are " + res);
					Utility.writeLog("No. of subsidieries added to the level " + i + " are " + res, "Info", finalTbl, "Level " + i + "Subsidiery Insertion", "db");
				}
				
				for(int j = 1; j < i; j++){
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (subsidiary_id, full_name, parent_id, parent_full_name,level,q_level) "
							+ " SELECT b.subsidiary_id, b.full_name,a.parent_id,a.parent_full_name, ? , ? FROM "
							+ "(SELECT subsidiary_id, full_name, parent_id, parent_full_name,level FROM "
							+ redShiftSchemaNamePreStage + "." + baseTbl + " WHERE level = ?) a, "
							+ "(SELECT subsidiary_id, full_name, parent_id, parent_full_name,level,q_level"
							+ " FROM "+ redShiftSchemaNamePreStage + "." + finalTbl + " WHERE Q_LEVEL = ? and LEVEL = ?) b "
							+ "WHERE a.subsidiary_id = b.parent_id";
					
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, j+1);
					ps.setInt(2, i);
					ps.setInt(3, i-j);
					ps.setInt(4, i);
					ps.setInt(5, j);
					res = ps.executeUpdate();
					System.out.println("No. of subsidieries added to this level are " + res);
					Utility.writeLog("No. of subsidieries added to the level " + i + " are " + res, "Info", finalTbl, "Level " + j+1 + "Subsidiery Insertion", "db");

				}
			}
			sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (subsidiary_id, full_name, parent_id, parent_full_name,level) "
					+ "SELECT DISTINCT subsidiary_id , full_name, subsidiary_id , full_name, 0 FROM " + redShiftSchemaNamePreStage + "." + baseTbl;
			
			ps = msCon.prepareStatement(sql);
			res = ps.executeUpdate();
			System.out.println("No. of subsidieries added to this level are " + res);
			Utility.writeLog("No. of subsidieries added to this level are " + res, "Info", finalTbl, "Final Level location Insertion", "db");


		} catch (SQLException e) {

			System.out.println("Error!!" + System.getProperty("line.separator")
					+ e.getMessage());
			
			msCon.rollback();
			
			Utility.writeLog("Error!!" + e.getMessage(), "Error", finalTbl, "Location Insertion", "db");
		}

	}

	
	
public void createAppTmpTbl() {
		
		Statement stmt = null;
		String sql;
		System.out.println("Temporary Table Creation is started...");
		try {
			stmt = msCon.createStatement();
			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + refTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creating Table " + refTbl + "...");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + refTbl + "(subsidiary_id integer,"
					+ "full_name varchar(1800), parent_id integer,parent_full_name varchar(1800) )";
					
			stmt.execute(sql);
			System.out.println("Done..");
			

			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + baseTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creating Table " + baseTbl + "...");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + baseTbl + "(subsidiary_id integer,"
					+ "full_name varchar(1800), parent_id integer,parent_full_name varchar(1800), level integer);";
			stmt.execute(sql);
			System.out.println("Done..");
			
			
			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + finalTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creating Table " + finalTbl + "...");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + finalTbl + "(subsidiary_id integer,"
					+ "full_name varchar(1800), parent_id integer,parent_full_name varchar(1800), level integer, q_level integer);";
			
			stmt.execute(sql);
			System.out.println("Done..");
			
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		
	}

public void populateAppTmpTbl() {
	
	Statement stmt = null;
	String sql;
	System.out.println("Populating table " + refTbl + " with the data of subsidieries table");
	try {
		stmt = msCon.createStatement();
		
		sql = "INSERT INTO " + redShiftSchemaNamePreStage + "." + refTbl 
				+ " SELECT a.subsidiary_id , a.full_name as full_name, a.parent_id, "
				+ "b.name as parent_full_name FROM "+ redShiftSchemaNamePreStage + ".subsidiaries a, " 
				+ redShiftSchemaNamePreStage + ".subsidiaries b "
				+ " WHERE a.parent_id = b.subsidiary_id(+);";
		
		int res = stmt.executeUpdate(sql);
		System.out.println(res + " rows are populated.");
		
		
	} catch(SQLException e) {
		e.printStackTrace();
	}
	
	
}

	public static void main(String args[]) {

		RelationerDataLoaderForSubsidiery j1 = new RelationerDataLoaderForSubsidiery();
		try {
			j1.processResult();
			//j1.addEmployeeBasedOnQLavel();
			
		} catch (SQLException e) {

			e.printStackTrace();
		}
	}

}
