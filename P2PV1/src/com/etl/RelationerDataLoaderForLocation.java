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

public class RelationerDataLoaderForLocation {

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
	
	public RelationerDataLoaderForLocation() {

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
		baseTbl = properties.getProperty("TBLBaseLoc");
		refTbl = properties.getProperty("TBLRefLoc");
		finalTbl = properties.getProperty("TBLFinalLoc");
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
			Utility.writeLog("Error!!! " + e.getMessage(), "Error", "Location", "Location_Hierarchy_Startup", "db");
			
		} catch (SQLException e) {
			
			System.out.println("Error!! " + e.getMessage());
			Utility.writeLog("Error!!! " + e.getMessage(), "Error", "Location", "Location_Hierarchy_Startup", "db");
		}

	}

	public void processResult() throws SQLException {
		String sql = "";
		PreparedStatement ps = null;
		int lvl = 0, tot  = 0; 
		System.out.println("Inserting prime locations to the organization database..");
		Utility.writeLog("Inserting prime locations to the organization database..", "Info", baseTbl, "Top Level Location Insertion", "db");
			try {
												
				sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
						+ baseTbl + "(location_id, location, address, "
						+ "parent_id, parent_location, parent_loc_address) "
						+ "SELECT location_id, location, address, parent_id, parent_location, parent_loc_address"
						+ " FROM "+ redShiftSchemaNamePreStage + "." + refTbl
					+ " WHERE (parent_id is null)";
				ps = msCon.prepareStatement(sql);
				
				tot = ps.executeUpdate();
				
				System.out.println("No of locations added to this level are " + tot); 
				Utility.writeLog("No of locations added to this level are " + tot, "Info", baseTbl, "Top Level Location Insertion", "db");
				
				addLocationBasedOnLavel(lvl + 1, lvl); 
				addLocationBasedOnQLavel();
				
				System.out.println("All locations are added to the database. Program will now exit.");
				
				Utility.writeLog("All locations are added to the database. Program will now exit.", "Info", finalTbl, "Location_Hierarchy_End", "db");
				msCon.commit();

			} catch (SQLException e) {
				
				System.out.println("Error!!\n" + e.getMessage());
				Utility.writeLog("Error !!" + e.getMessage(), "Error", "Location", "Location Insertion", "db");
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

	public void addLocationBasedOnLavel(int nxtLvl, int prvLvl) throws SQLException {

		String sql = "";
		PreparedStatement ps = null;
		int res = 0, lvl = prvLvl;
				 
		System.out.println("Inserting next level locations " + nxtLvl + " to the organization database..");
		Utility.writeLog("Inserting locations of level " + nxtLvl + " to the organization database..", "Info", baseTbl, "Level " + nxtLvl + " Location Insertion", "db");
		if(prvLvl == 0) {
		sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
				+ baseTbl
				+ " (location_id,location,address,parent_id,parent_location,parent_loc_address,level) "
				+ "SELECT b.location_id,b.location,b.address,b.parent_id,b.parent_location,b.parent_loc_address,1 FROM "
				+ redShiftSchemaNamePreStage + "." 
				+ baseTbl + " a," + redShiftSchemaNamePreStage + "." + refTbl + " b WHERE a.LEVEL is null and a.location_id = b.parent_id";
		
		ps = msCon.prepareStatement(sql);
		res = ps.executeUpdate();
		
		
		} else {
			
			sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
					+ baseTbl
					+ " (location_id,location,address,parent_id,parent_location,parent_loc_address,level) "
					+ "SELECT b.location_id,b.location,b.address,b.parent_id,b.parent_location,b.parent_loc_address,? FROM "
					+ redShiftSchemaNamePreStage + "." 
					+ baseTbl + " a," + redShiftSchemaNamePreStage + "." + refTbl + " b WHERE a.LEVEL = ? and a.location_id = b.parent_id";
			
			ps = msCon.prepareStatement(sql);
			ps.setInt(1, nxtLvl);
			ps.setInt(2, prvLvl);
			res = ps.executeUpdate();
			
		}

		try {

						
			System.out.println("No of locations added to the level:- " + nxtLvl + " are " + res);
			Utility.writeLog("No of locations added to the level:- " + nxtLvl + " are " + res, "Info", baseTbl, "Level " + nxtLvl + "Location Insertion", "db");

			if (res > 0) {
				lvl++;
				nxtLvl = lvl + 1;
				prvLvl = lvl;
				addLocationBasedOnLavel(nxtLvl, prvLvl);
				

			} else {
				hierarchyDepth = prvLvl;
				return;
			}

		} catch (SQLException e) {

			System.out.println("Error!!" + System.getProperty("line.separator")
					+ e.getMessage());
			
			Utility.writeLog("Error!! " + e.getMessage(), "Error", baseTbl, "Level " + nxtLvl + "Location Insertion", "db");
			
			msCon.rollback();
			
			
		}

	}
	
	public void addLocationBasedOnQLavel() throws SQLException {

		String sql = "";
		PreparedStatement ps = null;
		int res = 0;
		
					
		try {
			
			
			//LOOP THROUGH THE DEPTH OF THE LOCATION-PARENT LOCATION HIERERCHY 
			
			for(int i = 1; i <= hierarchyDepth; i++) {
				
				if(i == 1){
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (location_id,location,address,"
							+ "parent_id,parent_location,parent_loc_address,level,q_level) "
							+ "SELECT location_id, location, address,parent_id,parent_location,parent_loc_address, ?, ? "
							+ " FROM " + redShiftSchemaNamePreStage + "." + baseTbl + " WHERE LEVEL = ?";
													
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, i);
					ps.setInt(2, i-1);
					ps.setInt(3, i);
					
					res = ps.executeUpdate();
					System.out.println("No. of locations added to the level " + i + " are " + res);
					Utility.writeLog("No. of locations added to the level " + i + " are " + res, "Info", finalTbl, "Level " + i + "Location Insertion", "db");
					
				} else {
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (location_id,location,address,"
							+ "parent_id,parent_location,parent_loc_address,level,q_level) "
							+ "SELECT location_id, location, address,parent_id,parent_location,parent_loc_address, ?, ? "
							+ " FROM " + redShiftSchemaNamePreStage + "." + baseTbl + " WHERE LEVEL = ?";
													
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, 1);
					ps.setInt(2, i);
					ps.setInt(3, i);
					
					res = ps.executeUpdate();
					System.out.println("No. of locations added to the level " + i +" are " + res);
					Utility.writeLog("No. of locations added to the level " + i + " are " + res, "Info", finalTbl, "Level " + i + " Location Insertion", "db");
					
				}
				
				for(int j = 1; j < i; j++){
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (location_id,location,address,"
							+ "parent_id,parent_location,parent_loc_address,level,q_level) "
							+ " SELECT b.location_id, b.location,b.address,a.parent_id,a.parent_location,a.parent_loc_address, ? , ? FROM "
							+ "(SELECT location_id, location,address,parent_id,parent_location,parent_loc_address,level FROM "
							+ redShiftSchemaNamePreStage + "." + baseTbl + " WHERE level = ?) a, "
							+ "(SELECT location_id, location,address,parent_id,parent_location,parent_loc_address,level,q_level"
							+ " FROM "+ redShiftSchemaNamePreStage + "." + finalTbl + " WHERE Q_LEVEL = ? and LEVEL = ?) b "
							+ "WHERE a.location_id = b.parent_id";
					
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, j+1);
					ps.setInt(2, i);
					ps.setInt(3, i-j);
					ps.setInt(4, i);
					ps.setInt(5, j);
					
					res = ps.executeUpdate();
					System.out.println("No. of locations added to this level are " + res);
					Utility.writeLog("No. of locations added to the level " + i + " are " + res, "Info", finalTbl, "Level " + j+1 + " Location Insertion", "db");

				}
			}
			sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (location_id, location,address,parent_id,parent_location,parent_loc_address,level) "
					+ "SELECT DISTINCT location_id , location, address,location_id , location, address, 0 FROM " + redShiftSchemaNamePreStage + "." + baseTbl;
			
			ps = msCon.prepareStatement(sql);
			res = ps.executeUpdate();
			System.out.println("No. of locations added to this level are " + res);
			Utility.writeLog("No. of locations added to this level are " + res, "Info", finalTbl, "Final Level location Insertion", "db");


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
		Utility.writeLog("Temporary Table Creation is started..." , "Info", "Location", "Location_Hierarchy_Startup", "db");
		try {
			stmt = msCon.createStatement();
			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + refTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creating Table " + refTbl + "...");
			Utility.writeLog("Creating Table " + refTbl + "..." , "Info", "Location", "Location_Hierarchy_Startup", "db");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + refTbl + "(location_id integer,"
					+ "location varchar(800), address varchar(1000), parent_id integer,parent_location varchar(800),parent_loc_address varchar(1000) )";
					
			stmt.execute(sql);
			System.out.println("Done..");
			

			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + baseTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creating Table " + baseTbl + "...");
			Utility.writeLog("Creating Table " + baseTbl + "..." , "Info", "Location", "Location_Hierarchy_Startup", "db");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + baseTbl + "(location_id integer,"
					+ "location varchar(800), address varchar(1000), parent_id integer,parent_location varchar(800),parent_loc_address varchar(1000), level integer);";
			stmt.execute(sql);
			System.out.println("Done..");
			
			
			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + finalTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creating Table " + finalTbl + "...");
			Utility.writeLog("Creating Table " + finalTbl + "..." , "Info", "Location", "Location_Hierarchy_Startup", "db");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + finalTbl + "(location_id integer,"
					+ "location varchar(800), address varchar(1000), parent_id integer, parent_location varchar(800),parent_loc_address varchar(1000), level integer, q_level integer);";
			
			stmt.execute(sql);
			System.out.println("Done..");
			
			
		} catch (SQLException e) {
			
			System.out.println("Error!! " + e.getMessage());
			Utility.writeLog("Error!! " + e.getMessage(), "Error", "Location", "Location_Hierarchy_Startup", "db");
		}
		
	}

public void populateAppTmpTbl() {
	
	Statement stmt = null;
	String sql;
	System.out.println("Populating table " + refTbl + " with the data of location table");
	Utility.writeLog("Populating table " + refTbl + " with the data of location table" , "Info", "Location", "Location_Hierarchy_Startup", "db");
	try {
		stmt = msCon.createStatement();
		
		sql = "INSERT INTO " + redShiftSchemaNamePreStage + "." + refTbl 
				+ " SELECT a.location_id , a.name as location , a.address, a.parent_id, "
				+ "b.name as parent_location,b.address as parent_loc_address FROM "+ redShiftSchemaNamePreStage + ".locations a, " 
				+ redShiftSchemaNamePreStage + ".locations b "
				+ " WHERE a.parent_id = b.location_id(+);";
		
		int res = stmt.executeUpdate(sql);
		System.out.println(res + " rows are populated.");
		Utility.writeLog(res + " rows are populated.", "Info", "Location", "Location_Hierarchy_Startup", "db");
		
	} catch(SQLException e) {
		System.out.println("Error!! " + e.getMessage());
		Utility.writeLog("Error !!", "Error", "Location", "Location_Hierarchy_Startup", "db");
	}
	
	
}

	public static void main(String args[]) {

		RelationerDataLoaderForLocation j1 = new RelationerDataLoaderForLocation();
		try {
			j1.processResult();
			//j1.addEmployeeBasedOnQLavel();
			
		} catch (SQLException e) {

			e.printStackTrace();
		}
	}

}
