package de.cas.etl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.h2.util.IOUtils;

public class QueryBuilder {

	private CSVBuilder csvBuilder;
	
	public QueryBuilder() {

		csvBuilder = new CSVBuilder();
	}

	public void createNameDictionarie(Connection con) {
		ArrayList<String> result = new ArrayList<String>();
		try {
			Statement statement = con.createStatement();
			String query = 	"	SELECT 											" +
							"		DISTINCT NAME 								" +
							"		,ChristianName 								" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0]	";
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
			String query = 	"	SELECT 											" +
							"		DISTINCT Town1								" +
							" 	FROM [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0]	";
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
			String query = 	"	SELECT 											" +
							"		DISTINCT Country1 							" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0]	";
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
			String query = 	"	SELECT 											" +
							"		GroupNAME									" +
							"		,GID 										" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysGroup]	";
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
			String query = 	"	SELECT 											" +
							"		LoginNAME									" +
							"		, OID 										" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser]	";
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
			String query = 	"	SELECT 											" +
							"		UserNAME, 									" +
							"		OID 										" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser]	";
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
			String query =	"	SELECT 																						" +
							"		S1.OID 																					" +
							"		,DATEDIFF(DAY, '1.1.1990', E."+day+") AS 'Date' 										" +
							"		,"+n+" AS Typ 																			" +
							"		,S2.OID 																				" +
							"		,A2.gwIsCompany 																		" +
							"		,A2.gwIsContact 																		" +
							"		,A2.gwIsEmployee 																		" +
							"		,A2.Town1 																				" +
							"		,A2.Country1 																			" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 											" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1 			" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"0] E ON E.GGUID = R.GUID2 			" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER ON ER.TableGUID = E.GGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER2 ON ER2.TableGUID = ER.TableGUID	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON ER.OID = S2.OID 					" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID 	" +
							"	WHERE (R.TableSign2 = '"+type+"') 															" +
							"		AND ER.OID > 0 																			" +
							"		AND ER.OID != ER2.OID																	" +																
							"		AND ER.TableGUID NOT IN (	SELECT TableGUID 											" +	
							"									FROM [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] 		" +			
							"									Group by TableGUID 											" +			
							"									HAVING COUNT(*) > 15) 										" +
							"																								" +
							"	UNION ALL 																					" +
							"																								" +
							"	SELECT 																						" +
							"		S1.OID 																					" +
							"		,DATEDIFF(DAY, '1.1.1990', E."+day+") AS 'Date' 										" +
							"		,"+n+" AS Typ 																			" +
							"		,S2.OID 																				" +
							"		,A2.gwIsCompany 																		" +
							"		,A2.gwIsContact 																		" +
							"		,A2.gwIsEmployee 																		" +
							"		,A2.Town1 																				" +
							"		,A2.Country1 																			" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 											" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1  			" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"0] E ON E.GGUID = R.GUID2 			" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER ON ER.TableGUID = E.GGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER2 ON ER2.TableGUID = ER.TableGUID	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG2 ON ER.OID = SG2.GID 					" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM2 ON SGM2.GroupID = SG2.GGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON SGM2.MEMBERID = S2.GGUID 			" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID 	" +
							"	WHERE (R.TableSign2 = '"+type+"') 															" +
							"		AND ER.OID < 0 																			" +
							"		AND ER.OID != ER2.OID																	" +
							"		AND ER.TableGUID NOT IN (	SELECT TableGUID 											" +	
							"									FROM [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] 		" +			
							"									Group by TableGUID 											" +			
							"									HAVING COUNT(*) > 15) 										" +
							"	ORDER BY S1.OID																				";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[] { rsSET.getString(1), rsSET.getString(2), rsSET.getString(3),
						rsSET.getString(4), rsSET.getString(5), rsSET.getString(6), rsSET.getString(7),
						rsSET.getString(8), rsSET.getString(9)});
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
			String query =	"	SELECT 																						" +
							"		S1.OID																					" +
							"		,DATEDIFF(DAY, '1.1.1990', E.start_dt) AS 'Date' 										" +
							"		,"+n+" AS Typ 																			" +
							"		,S2.OID																					" +
							"		,A2.gwIsCompany 																		" +
							"		,A2.gwIsContact 																		" +
							"		,A2.gwIsEmployee 																		" +
							"		,A2.Town1 																				" +
							"		,A2.Country1 																			" +
							"		,DATEDIFF(d, start_dt, End_dt) AS DIFF 													" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 											" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1 			" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"0] E ON E.GGUID = R.GUID2 			" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER ON ER.TableGUID = E.GGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER2 ON ER2.TableGUID = ER.TableGUID	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON ER.OID = S2.OID 					" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID 	" +
							"	WHERE (R.TableSign2 = '"+type+"') 															" +
							"		AND ER.OID > 0 																			" +
							"		AND ER.OID != ER2.OID																	" +
							"		AND ER.TableGUID NOT IN (	SELECT TableGUID 											" +	
							"									FROM [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] 		" +			
							"									Group by TableGUID 											" +			
							"									HAVING COUNT(*) > 15) 										" +
							"		AND DATEDIFF(d, E.start_dt, E.End_dt) > 1 AND DATEDIFF(d, E.start_dt, E.End_dt) < 30 	" +
							"																								" +
							"	UNION ALL 																					" +
							"																								" +
							"	SELECT 																						" +
							"		S1.OID, 																				" +
							"		DATEDIFF(DAY, '1.1.1990', E.start_dt) AS 'Date' 										" + 
							"		,"+n+" AS Typ 																			" +
							"		,S2.OID 																				" +
							"		,A2.gwIsCompany 																		" +
							"		,A2.gwIsContact 																		" +
							"		,A2.gwIsEmployee 																		" +
							"		,A2.Town1 																				" +
							"		,A2.Country1 																			" +
							"		,DATEDIFF(d, start_dt, End_dt) AS DIFF 													" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 											" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1  			" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"0] E ON E.GGUID = R.GUID2 			" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER ON ER.TableGUID = E.GGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] ER2 ON ER2.TableGUID = ER.TableGUID	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG2 ON ER.OID = SG2.GID 					" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM2 ON SGM2.GroupID = SG2.GGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON SGM2.MEMBERID = S2.GGUID 			" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID 	" +
							"	WHERE (R.TableSign2 = '"+type+"') 															" +
							"		AND ER.OID < 0 																			" +
							"		AND ER.OID != ER2.OID																	" +
							"		AND ER.TableGUID NOT IN (	SELECT TableGUID 											" +	
							"									FROM [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] 		" +			
							"									Group by TableGUID 											" +			
							"									HAVING COUNT(*) > 15) 										" +
							"		AND DATEDIFF(d, E.start_dt, E.End_dt) > 1 AND DATEDIFF(d, E.start_dt, E.End_dt) < 30	";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[] { rsSET.getString(1), rsSET.getString(2), rsSET.getString(3),
						rsSET.getString(4), rsSET.getString(5), rsSET.getString(6), rsSET.getString(7),
						rsSET.getString(8), rsSET.getString(9), rsSET.getString(10)});
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
			String query = 	"	SELECT 																							" +
							"		S1.OID																		 				" +
							"		,DATEDIFF(DAY, '1.1.1990', replace(C.OldFieldValue, ',', ' ')) AS 'Date' 					" +
							"		,"+n+" AS Typ 																				" +
							"		,S2.OID 																					" +
							"		,A2.gwIsCompany 																			" +
							"		,A2.gwIsContact 																			" +
							"		,A2.gwIsEmployee 																			" +
							"		,A2.Town1 																					" +
							"		,A2.Country1 																				" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 												" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID			" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1 				" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENT0] E ON E.GGUID = R.GUID2 					" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[ChangeLogBook] C ON C.TableGUID = E.GGUID				" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] ER ON ER.TableGUID = E.GGUID			" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] ER2 ON ER2.TableGUID = ER.TableGUID	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON ER.OID = S2.OID 						" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID			" +
							"	WHERE (R.TableSign2 = 'APP') 																	" +
							"		AND ER.OID > 0																				" +
							"		AND ER.OID != ER2.OID																		" +
							"		AND ER.TableGUID NOT IN (	SELECT TableGUID 												" +	
							"									FROM [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] 			" +			
							"									Group by TableGUID 												" +			
							"									HAVING COUNT(*) > 40) 											" +
							"		AND C.TableName = 'APPOINTMENT' 															" +
							"		AND C.FieldName = 'START_DT' 																" +
							"		AND C.ChangeType = 'U'																		" +
							"		AND convert(varchar, C.UpdateTimestamp, 21) > replace(C.OldFieldValue, ',', ' ') 			" +
							"		AND convert(varchar, C.UpdateTimestamp, 21) < replace(C.NewFieldValue, ',', ' ') 			" +
							"		AND substring(C.OldFieldValue, 1, 10) <> substring(C.NewFieldValue, 1, 10) 					" +
							"																									" +
							" 	UNION ALL 																						" +
							"																									" +
							"	SELECT 																							" +
							"		S1.OID 																						" +
							"		,DATEDIFF(DAY, '1.1.1990', replace(C.OldFieldValue, ',', ' ')) AS 'Date' 					" +
							"		,"+n+" AS Typ	 																			" +
							"		,S2.OID																						" +
							"		,A2.gwIsCompany																				" +
							"		,A2.gwIsContact																				" +
							"		,A2.gwIsEmployee																			" +
							"		,A2.Town1																					" +
							"		,A2.Country1 																				" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 												" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A1 ON A1.SysUserGUID = S1.GGUID 		" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[TableRelation] R ON A1.GGUID = R.GUID1 				" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENT0] E ON E.GGUID = R.GUID2 					" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[ChangeLogBook] C ON C.TableGUID = E.GGUID 				" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] ER ON ER.TableGUID = E.GGUID 			" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] ER2 ON ER2.TableGUID = ER.TableGUID	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG2 ON ER.OID = SG2.GID 						" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM2 ON SGM2.GroupID = SG2.GGUID 		" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S2 ON SGM2.MEMBERID = S2.GGUID 				" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A2 ON A2.SysUserGUID = S2.GGUID 		" +
							"	WHERE (R.TableSign2 = 'APP') AND ER.OID < 0 													" +
							"		AND ER.OID != ER2.OID																		" +
							"		AND ER.TableGUID NOT IN (	SELECT TableGUID 												" +	
							"									FROM [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] 			" +			
							"									Group by TableGUID 												" +			
							"									HAVING COUNT(*) > 40) 											" +
							"		AND C.TableName = 'APPOINTMENT' 															" +
							"		AND C.FieldName = 'START_DT' 																" +
							"		AND C.ChangeType = 'U' 																		" +
							"		AND convert(varchar, C.UpdateTimestamp, 21) > replace(C.OldFieldValue, ',', ' ') 			" +
							"		AND convert(varchar, C.UpdateTimestamp, 21) < replace(C.NewFieldValue, ',', ' ') 			" +
							"		AND substring(C.OldFieldValue, 1, 10) <> substring(C.NewFieldValue, 1, 10)					";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[] { rsSET.getString(1), rsSET.getString(2), rsSET.getString(3),
						rsSET.getString(4), rsSET.getString(5), rsSET.getString(6), rsSET.getString(7),
						rsSET.getString(8), rsSET.getString(9)});
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
	
	public void buildGetGroupChanges(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = 	"	SELECT 																	" +
							"		DATEDIFF(DAY, '1.1.1990', UpdateTimestamp) AS dt 					" +
	                        "		,(CASE WHEN FieldName = 'AddUsersToGroup' THEN 'A' ELSE 'D' END) 	" +
	                        "		,NewFieldValue 														" +
	                        "		,IsMemo																" +
	                        "		,GGUID 																" +
                            "	FROM [genesisWorldDB_Haus_x5].[dbo].[ChangeLogBook] 					" +
                            "	WHERE TableName = 'SYSGROUP' 											" +
                            "		AND FieldName IN('AddUsersToGroup', 'DeleteUsersFromGroup') 		" +
                            "		AND NewFieldValue IS NOT NULL 										" +
                            "		AND NewFieldValue LIKE '%UserGUIDs%' 								" +
                            "		AND UpdateTimestamp > '30.6.2006' 									" +
                            "	ORDER BY dt, IDNr 														";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {		
				
					String[] container = new String[4];
				
					
					if(rsSET.getInt(4) == 1) {
						
						byte[] gguid = rsSET.getBytes(5);
						
						String queryMemo = 	"	SELECT MemoFieldValue 								" +
                                			"	FROM [genesisWorldDB_Haus_x5].[dbo].[MemoLogBook] 	" +
                                			"	WHERE ChangeLogBookGUID = ? 						" +
                                			"		AND IsNew = 1 									";
						
						PreparedStatement preStatement = con.prepareStatement(queryMemo);
						preStatement.setBytes(1, gguid);
						
						ResultSet rsNew = preStatement.executeQuery();
						
						while(rsNew.next()) {
							int indexStart = rsNew.getString(1).indexOf("~|vaUserGUIDs~^8##") + 18;
							int indexEnd = rsNew.getString(1).indexOf("~", indexStart);
							String eStart = rsNew.getString(1).substring(indexStart, indexEnd);
							String[] sSTart = eStart.split("\n");
							
							
							int indexStartGroup = rsNew.getString(1).indexOf("~|GroupGUID~^8##") + 16;
							int indexEndGroup = rsNew.getString(1).indexOf("~", indexStartGroup);
							
							for(String s : sSTart) {
								container[0] = rsSET.getString(1);
								container[1] = rsSET.getString(2);
								container[2] = s.replace("\n", "").replace("\r", "");
								container[3] = rsNew.getString(1).substring(indexStartGroup, indexEndGroup);
							}
						}
					}
					else {
						
						rsSET.getString(1);
						int indexStartUser = rsSET.getString(3).indexOf("~|vaUserGUIDs~^8##") + 18;
						int indexEndUser = rsSET.getString(3).indexOf("~", indexStartUser);

						String eStart = rsSET.getString(3).substring(indexStartUser, indexEndUser);
						String[] sSTart = eStart.split("\n");
						
						int indexStartGroup = rsSET.getString(3).indexOf("~|GroupGUID~^8##") + 16;
						int indexEndGroup = rsSET.getString(3).indexOf("~", indexStartGroup);
						
						for(String s : sSTart) {
							container[0] = rsSET.getString(1);
							container[1] = rsSET.getString(2);
							container[2] = s.replace("\n", "").replace("\r", "");
							container[3] = rsSET.getString(3).substring(indexStartGroup, indexEndGroup);
						}
					}
					
					if(!container[2].equals("")) {
						result.add(container);
					}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildGroupHistory(result);
	}
	
	public void buildUserGGUID(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT GGUID, OID FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser]";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[]{"0x"+rsSET.getString(1), rsSET.getString(2)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}	
		csvBuilder.buildGGUIDDictionarie("GGUID_User", result);
	}
	
	public void buildGroupGGUID(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT GGUID, GID FROM [genesisWorldDB_Haus_x5].[dbo].[SysGroup]";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[]{"0x"+rsSET.getString(1), rsSET.getString(2)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildGGUIDDictionarie("GGUID_Group", result);
	}
	
	public void buildSysUserAndGroupWithAdress(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = 	"	SELECT 																					" + 
							"		SG.GID 																				" +
							"		,S.OID																				" +
							"		,A.gwIsCompany 																		" +
							"		,A.gwIsContact 																		" +
							"		,A.gwIsEmployee 																	" +
							"		,A.Town1 																			" +
							"		,A.Country1 																		" + 
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM1 								" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S ON SGM1.MEMBERID = S.GGUID 			" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG ON SGM1.GroupID = SG.GGUID 		" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A ON S.GGUID = A.SysUserGUID 	" +
							"	ORDER BY SG.GID "; 
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[]{rsSET.getString(1), rsSET.getString(2), rsSET.getString(3),
						rsSET.getString(4), rsSET.getString(5), rsSET.getString(6),  rsSET.getString(7)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		for(int i = 0; i < result.size(); ++i) {			
			result.get(i)[2] = result.get(i)[2] == null || result.get(i)[2].equals("") ? "0" : result.get(i)[2];
			result.get(i)[3] = result.get(i)[3] == null  || result.get(i)[3].equals("") ? "0" : result.get(i)[3];
			result.get(i)[4] = result.get(i)[4] == null  || result.get(i)[4].equals("") ? "1" : result.get(i)[4];
			result.get(i)[5] = result.get(i)[5] == null  || result.get(i)[5].equals("") ? "0" : result.get(i)[5];
			result.get(i)[6] = result.get(i)[6] == null  || result.get(i)[6].equals("") ? "0" : result.get(i)[6];
		}
		csvBuilder.buildSysUserWithAdress("SysUserAndGroupWithAdress", result);
	}
	
	public void buildSysUserWithAdress(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = 	"	SELECT 																			" +
							"		S.OID 																		" +
							"		,A.gwIsCompany 																" +
							"		,A.gwIsContact 																" +
							"		,A.gwIsEmployee 															" +
							"		,A.Town1 																	" +
							"		,A.Country1 																" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S 								" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[ADDRESS0] A ON S.GGUID = A.SysUserGUID " +
							"	ORDER BY S.OID  	";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[]{rsSET.getString(1), rsSET.getString(2), rsSET.getString(3),
						rsSET.getString(4), rsSET.getString(5), rsSET.getString(6)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildSysUserWithAdress("SysUserWithAdress", result);
	}
	
	public void buildSysGroupRelation(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query =	"	SELECT 																						" +
							"		[GroupNAME]																					" +
							"		,[OID] 																					" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[SysUser] S1 											" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM2 ON SGM2.MEMBERID = S1.GGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG2 ON SG2.GGUID = SGM2.GroupID" +
							"	ORDER BY [GID]			";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[]{rsSET.getString(1), rsSET.getString(2)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildSysUserWithAdress("SysGroupRelation", result);
	}
	
	public void build0RelData(Connection con, String table, String date, String type) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		
		String phoneWhere = "";
		
		if(table.equals("gwPhoneCall")) {
			phoneWhere = 	" 	AND R2.OID <> 0 													" +
	                        "	AND A1.Start_dt IS NOT NULL AND A1.end_dt IS NOT NULL 				" +
	                        "	AND A1.DialledNumber <> '' AND A1.DialledNumber <> 'anonymous' 		" +
	                        "	AND A1.CustomerShortInfo <> 'keine Adresse zur Rufnummer gefunden' 	" +
	                        "	AND A1.duration > 0													";
		}
		
		try {
			Statement statement = con.createStatement();
			String query = 	"	SELECT																						" +
							"		R2.[OID] 																				" +
							"		,DATEDIFF(DAY, '1.1.1990', A1."+date+") AS 'Date' 										" +
							"		, "+type+" 																				" +
							"		,CASE WHEN R3.OID < 0 THEN S.OID ELSE R3.[OID]  END AS OID								" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].["+table+"0] A1 										" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] R2 ON A1.GGUID = R2.TableGUID 		" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] R3 ON R2.TableGUID = R3.TableGUID 	" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG ON SG.GID = R3.OID				" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM ON SGM.GroupID = SG.GGUID	" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S ON S.GGUID = SGM.MEMBERID			" +
							"	WHERE A1."+date+" IS NOT null 																" +
							"		AND R2.OID != R3.OID 																	" +
							"		AND R2.OID > 0																			" +
							"		AND R2.TableGUID NOT IN (	SELECT TableGUID 											" + 
							"									FROM [genesisWorldDB_Haus_x5].[dbo].["+table+"ORel] 		" +
							"									Group by TableGUID 											" +
							"									HAVING COUNT(*) > 15) 										" + 
							""
							+ phoneWhere;
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[]{rsSET.getString(1), rsSET.getString(2), rsSET.getString(3),
						rsSET.getString(4), "null", "null", "null", "null", "null"});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildSysUserWithAdress(table+"ORel", result);
	}
	
	public void build0RelAppointmentMultipleDays(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query =	"	SELECT 																							" +
							"		R2.[OID] 																					" +
							"		,DATEDIFF(DAY, '1.1.1990', A1.start_dt) AS 'Date' 											" +
							"		, 3 																						" +
							"		,CASE WHEN R3.OID < 0 THEN S.OID ELSE R3.[OID]  END AS OID									" +
							"		,DATEDIFF(d, A1.start_dt, A1.End_dt) AS DIFF 												" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENT0] A1 											" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] R2 ON A1.GGUID = R2.TableGUID 		" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] R3 ON R2.TableGUID = R3.TableGUID 	" + 
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG ON SG.GID = R3.OID					" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM ON SGM.GroupID = SG.GGUID		" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S ON S.GGUID = SGM.MEMBERID				" +
							"	WHERE A1.[start_dt] IS NOT null 																" +
							"		AND R2.OID != R3.OID																		" +
							"		AND R2.OID > 0 																				" +
							"		AND R2.TableGUID NOT IN (	SELECT TableGUID 												" + 
							"									FROM [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] 			" +
							"									Group by TableGUID 												" +
							"									HAVING COUNT(*) > 15) 											" + 
							"		AND DATEDIFF(d, A1.start_dt, A1.End_dt) > 1 AND DATEDIFF(d, A1.start_dt, A1.End_dt) < 30 	" +
							"";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[]{rsSET.getString(1), rsSET.getString(2), rsSET.getString(3),
						rsSET.getString(4), rsSET.getString(5), "null", "null", "null", "null", "null"});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildSysUserWithAdress("Splitted_APPOINTMENTORel", result);
	}
	
	public void build0RelAppointmentDayShifts(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query =	"	SELECT 																						" +
							"		R2.[OID] 																				" +
							"		,DATEDIFF(DAY, '1.1.1990', replace(C.OldFieldValue, ',', ' ')) AS 'Date' 				" +
							"		, 3 																					" +
							"		,CASE WHEN R3.OID < 0 THEN S.OID ELSE R3.[OID]  END AS OID								" +
							"	FROM [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENT0] A1 										" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[ChangeLogBook] C ON C.TableGUID = A1.GGUID 		" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] R2 ON A1.GGUID = R2.TableGUID 	" +
							"		JOIN [genesisWorldDB_Haus_x5].[dbo].[APPOINTMENTORel] R3 ON R2.TableGUID = R3.TableGUID " +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroup] SG ON SG.GID = R3.OID				" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[SysGroupMember] SGM ON SGM.GroupID = SG.GGUID	" +
							"		LEFT JOIN [genesisWorldDB_Haus_x5].[dbo].[SysUser] S ON S.GGUID = SGM.MEMBERID			" +
							"	WHERE  C.TableName = 'APPOINTMENT' 															" + 
							"		AND R2.OID != R3.OID																	" +
							"		AND R2.OID > 0 																			" +
							"		AND C.FieldName = 'START_DT' 															" +  
							"		AND C.ChangeType = 'U' 																	" +  
							"		AND convert(varchar, C.UpdateTimestamp, 21) > replace(C.OldFieldValue, ',', ' ') 		" +  
							"		AND convert(varchar, C.UpdateTimestamp, 21) < replace(C.NewFieldValue, ',', ' ') 		" +  
							"		AND substring(C.OldFieldValue, 1, 10) <> substring(C.NewFieldValue, 1, 10)				" +
							"";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[]{rsSET.getString(1), rsSET.getString(2), rsSET.getString(3),
						rsSET.getString(4), "null", "null", "null", "null", "null"});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		csvBuilder.buildSysUserWithAdress("Old_APPOINTMENTORel", result);
	}
}
