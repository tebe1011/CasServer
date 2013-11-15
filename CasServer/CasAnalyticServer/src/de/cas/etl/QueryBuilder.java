package de.cas.etl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class QueryBuilder {

	private CSVBuilder csvBuilder;
	
	public QueryBuilder() {

		csvBuilder = new CSVBuilder();
	}

	public void createNameDictionarie(Connection con) {
		ArrayList<String> result = new ArrayList<String>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT DISTINCT NAME , ChristianName FROM [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0]";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(rsSET.getString(1) + " " + rsSET.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildDictionarie("NAME", result);
	}

	public void createTown1Dictionarie(Connection con) {
		ArrayList<String> result = new ArrayList<String>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT DISTINCT Town1 FROM [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0]";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(rsSET.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildDictionarie("Town", result);
	}

	public void createCountry1Dictionarie(Connection con) {
		ArrayList<String> result = new ArrayList<String>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT DISTINCT Country1 FROM [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0]";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(rsSET.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildDictionarie("Country", result);
	}
	
	public void createGroupDictionarie(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT GroupNAME, GID FROM [genesisWorldDB_Haus_x5].[dbo].[SysGroup]";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[] {rsSET.getString(1), rsSET.getString(2)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildDictionarieWithOID("SysGroup", result);
	}
	
	public void createSysUserDictionarie(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT LoginNAME, OID FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser]";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[]{rsSET.getString(1), rsSET.getString(2)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildDictionarieWithOID("SysUser", result);
	}
	
	public void createClientUser(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT UserNAME, OID FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser]";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[]{rsSET.getString(1), rsSET.getString(2)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildDictionarieWithOID("ClientUser", result);
	}
	
	public void buildObjektData(Connection con, String table, String type, String day, String n) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT S1.OID, " +
					"DATEDIFF(DAY, '1.1.1990', E."+day+") AS 'Date', " +
					""+n+" AS Typ, " +
					"S2.OID, " +
					"A2.gwIsCompany, " +
					"A2.gwIsContact, " +
					"A2.gwIsEmployee, " +
					"A2.Town1, " +
					"A2.Country1, " +
					"SG2.GID " +
					"FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1 " +
					"LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"0] E ON E.GGUID = R.GUID2 " +
					"LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER ON ER.TableGUID = E.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON ER.OID = S2.OID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM2 ON SGM2.MEMBERID = S2.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG2 ON SG2.GGUID = SGM2.GroupID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID " +
					"WHERE (R.TableSign2 = '"+type+"') " +
					"AND ER.OID > 0 " +
					"AND S1.OID NOT LIKE S2.OID " +
					"" +
					"UNION ALL " +
					"" +
					"SELECT S1.OID, " +
					"DATEDIFF(DAY, '1.1.1990', E."+day+") AS 'Date', " +
					""+n+" AS Typ, " +
					"S2.OID, " +
					"A2.gwIsCompany, " +
					"A2.gwIsContact, " +
					"A2.gwIsEmployee, " +
					"A2.Town1, " +
					"A2.Country1, " +
					"SG2.GID " +
					"FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1  " +
					"LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"0] E ON E.GGUID = R.GUID2 " +
					"LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER ON ER.TableGUID = E.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG2 ON ER.OID = SG2.GID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM2 ON SGM2.GroupID = SG2.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON SGM2.MEMBERID = S2.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID " +
					"WHERE (R.TableSign2 = '"+type+"') " +
					"AND ER.OID < 0 " +
					"AND S1.OID NOT LIKE S2.OID " +
					"ORDER BY S1.OID";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[] { rsSET.getString(1), rsSET.getString(2), rsSET.getString(3),
						rsSET.getString(4), rsSET.getString(5), rsSET.getString(6), rsSET.getString(7),
						rsSET.getString(8), rsSET.getString(9), rsSET.getString(10)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildTableRelation(table, result);
	}
	
	public void buildDataWhichGoesOverADay(Connection con, String table, String type, String n) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT S1.OID, " +
					"DATEDIFF(DAY, '1.1.1990', E.start_dt) AS 'Date', " +
					""+n+" AS Typ, " +
					"S2.OID, " +
					"A2.gwIsCompany, " +
					"A2.gwIsContact, " +
					"A2.gwIsEmployee, " +
					"A2.Town1, " +
					"A2.Country1, " +
					"SG2.GID, " +
					"DATEDIFF(d, start_dt, End_dt) AS DIFF " +
					"FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1 " +
					"LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"0] E ON E.GGUID = R.GUID2 " +
					"LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER ON ER.TableGUID = E.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON ER.OID = S2.OID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM2 ON SGM2.MEMBERID = S2.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG2 ON SG2.GGUID = SGM2.GroupID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID " +
					"WHERE (R.TableSign2 = '"+type+"') " +
					"AND ER.OID > 0 " +
					"AND S1.OID NOT LIKE S2.OID " +
					"AND DATEDIFF(d, E.start_dt, E.End_dt) > 1 AND DATEDIFF(d, E.start_dt, E.End_dt) < 30 " +
					"" +
					"UNION ALL " +
					"" +
					"SELECT S1.OID, " +
					"DATEDIFF(DAY, '1.1.1990', E.start_dt) AS 'Date', " + 
					""+n+" AS Typ, " +
					"S2.OID, " +
					"A2.gwIsCompany, " +
					"A2.gwIsContact, " +
					"A2.gwIsEmployee, " +
					"A2.Town1, " +
					"A2.Country1, " +
					"SG2.GID, " +
					"DATEDIFF(d, start_dt, End_dt) AS DIFF " +
					"FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1  " +
					"LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"0] E ON E.GGUID = R.GUID2 " +
					"LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER ON ER.TableGUID = E.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG2 ON ER.OID = SG2.GID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM2 ON SGM2.GroupID = SG2.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON SGM2.MEMBERID = S2.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID " +
					"WHERE (R.TableSign2 = '"+type+"') " +
					"AND ER.OID < 0 " +
					"AND S1.OID NOT LIKE S2.OID " +
					"AND DATEDIFF(d, E.start_dt, E.End_dt) > 1 AND DATEDIFF(d, E.start_dt, E.End_dt) < 30";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[] { rsSET.getString(1), rsSET.getString(2), rsSET.getString(3),
						rsSET.getString(4), rsSET.getString(5), rsSET.getString(6), rsSET.getString(7),
						rsSET.getString(8), rsSET.getString(9), rsSET.getString(10), rsSET.getString(11)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildTableRelation("Splitted_"+table, result);
	}
	
	public void buildAppointmentTimeShifts(Connection con, String n) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT S1.OID, " +
					"DATEDIFF(DAY, '1.1.1990', replace(C.OldFieldValue, ',', ' ')) AS 'Date', " +
					""+n+" AS Typ, " +
					"S2.OID, " +
					"A2.gwIsCompany, " +
					"A2.gwIsContact, " +
					"A2.gwIsEmployee, " +
					"A2.Town1, " +
					"A2.Country1, " +
					"SG2.GID " +
					"FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID	" +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1 " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENT0] E ON E.GGUID = R.GUID2 " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ChangeLogBook] C ON C.TableGUID = E.GGUID	" +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] ER ON ER.TableGUID = E.GGUID	" +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON ER.OID = S2.OID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM2 ON SGM2.MEMBERID = S2.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG2 ON SG2.GGUID = SGM2.GroupID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID	" +
					"WHERE (R.TableSign2 = 'APP') " +
					"AND C.TableName = 'APPOINTMENT' " +
					"AND C.FieldName = 'START_DT' " +
					"AND ER.OID > 0	" +
					"AND S1.OID NOT LIKE S2.OID " +
					"AND C.ChangeType = 'U'	" +
					"AND convert(varchar, C.UpdateTimestamp, 21) > replace(C.OldFieldValue, ',', ' ') " +
					"AND convert(varchar, C.UpdateTimestamp, 21) < replace(C.NewFieldValue, ',', ' ') " +
					"AND substring(C.OldFieldValue, 1, 10) <> substring(C.NewFieldValue, 1, 10) " +
					"" +
					"UNION ALL " +
					" " +
					"SELECT S1.OID, " +
					"DATEDIFF(DAY, '1.1.1990', replace(C.OldFieldValue, ',', ' ')) AS 'Date', " +
					""+n+" AS Typ, " +
					"S2.OID, " +
					"A2.gwIsCompany, " +
					"A2.gwIsContact, " +
					"A2.gwIsEmployee, " +
					"A2.Town1, " +
					"A2.Country1, " +
					"SG2.GID " +
					"FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1 " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENT0] E ON E.GGUID = R.GUID2 " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ChangeLogBook] C ON C.TableGUID = E.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] ER ON ER.TableGUID = E.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG2 ON ER.OID = SG2.GID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM2 ON SGM2.GroupID = SG2.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON SGM2.MEMBERID = S2.GGUID " +
					"JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID " +
					"WHERE (R.TableSign2 = 'APP') AND ER.OID < 0 " +
					"AND S1.OID NOT LIKE S2.OID " +
					"AND C.TableName = 'APPOINTMENT' " +
					"AND C.FieldName = 'START_DT' " +
					"AND C.ChangeType = 'U' " +
					"AND convert(varchar, C.UpdateTimestamp, 21) > replace(C.OldFieldValue, ',', ' ') " +
					"AND convert(varchar, C.UpdateTimestamp, 21) < replace(C.NewFieldValue, ',', ' ') " +
					"AND substring(C.OldFieldValue, 1, 10) <> substring(C.NewFieldValue, 1, 10)";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[] { rsSET.getString(1), rsSET.getString(2), rsSET.getString(3),
						rsSET.getString(4), rsSET.getString(5), rsSET.getString(6), rsSET.getString(7),
						rsSET.getString(8), rsSET.getString(9), rsSET.getString(10)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildTableRelation("Old_APPOINTMENT", result);
	}
	
	public String checkUserInDatabase(Connection con, String username) {
		String id = "null";
		try {
		Statement statement = con.createStatement();
		String query = "SELECT id FROM D_SysUser WHERE name = '"+username.toUpperCase()+"'";
		ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				id = rsSET.getString(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return id;
	}
}
