package de.cas.etl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Load {

	private String transformPath = System.getProperty("catalina.base") + "/CasAnalyticsData/Transform/";
	private String extractPath = System.getProperty("catalina.base") + "/CasAnalyticsData/Extract/";

	public Load() {

	}

	public void loadMainData(Connection con) {
		try {
			con.createStatement().executeUpdate(
					"INSERT INTO data SELECT * FROM CSVREAD('" + transformPath + "reunited_sorted_replaced.csv');");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void loadTownData(Connection con) {
		try {
			con.createStatement().executeUpdate(
					"INSERT INTO D_Town SELECT * FROM CSVREAD('" + extractPath + "Town.csv');");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void loadCountryData(Connection con) {
		try {
			con.createStatement().executeUpdate(
					"INSERT INTO D_Country SELECT * FROM CSVREAD('" + extractPath + "Country.csv');");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void loadSysUserData(Connection con) {
		try {
			con.createStatement().executeUpdate(
					"INSERT INTO D_SysUser SELECT * FROM CSVREAD('" + extractPath + "SysUser.csv');");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void loadSysUserGroupData(Connection con) {
		try {
			con.createStatement().executeUpdate(
					"INSERT INTO D_SysUserGroup SELECT * FROM CSVREAD('" + extractPath + "SysGroup.csv');");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void createIndexPersonID(Connection con) {
		try {
			con.createStatement().executeUpdate(
					"CREATE INDEX IDXPERSONID ON DATA(userID);");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void createIndexDateDay(Connection con) {
		try {
			con.createStatement().executeUpdate(
					"CREATE INDEX IDXDATEDAY ON DATA(DateDay);");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void createIndexOTyp(Connection con) {
		try {
			con.createStatement().executeUpdate(
					"CREATE INDEX IDXOTYP ON DATA(OTyp);");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
