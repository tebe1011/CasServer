package de.cas.etl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Logik {

	private ArrayList<String> whereStatements = new ArrayList<String>();

	public Logik() {

	}

	public JSONObject buildQuery(Connection con, JSONObject json) {
		
		double wDoc = 0.00;
		double wEml = 0.00;
		double wApp = 0.00;
		double wGwop = 0.00;
		double wPhc = 0.00;
		
		int range = 0;
		
		long startDiff = 0;
		long endDiff = 0;
		long startDay = 0;
		long startDayPlus = 0;
		long endDay = 0;
		long endDayPlus = 0;
		double faktorStart = 1.0;
		double faktorEnd = 1.0;
		
		try {
			DateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

			startDiff = json.getInt("TextField_IntervallStart");
			endDiff = json.getInt("TextField_IntervalLEnd");
			
			String start = json.getString("DateField_Start");
			String end = json.getString("DateField_End");

			Date dateStart = formatter.parse(start);
			Date dateEnd = formatter.parse(end);
			Date dateBeginn = formatter.parse("MON JAN 01 00:00:00 CET 1990");

			long startFinal = getDateDiff(dateBeginn, dateStart, TimeUnit.DAYS);
			long endFinal = getDateDiff(dateBeginn, dateEnd, TimeUnit.DAYS);
			
			startDay = startFinal;
			startDayPlus = startFinal + startDiff;
			endDay = endFinal;
			endDayPlus = endFinal - endDiff;
			
			if(startDiff != 0) {
				faktorStart = 1.00000/startDiff;
			}
			if(endDiff != 0) {
				faktorEnd = 1.00000/endDiff;
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		try {
			wDoc = json.getDouble("Slider_Document") / 100.00;
			wEml = json.getDouble("Slider_Email") / 100.00;
			wApp = json.getDouble("Slider_Appointment") / 100.00;
			wGwop = json.getDouble("Slider_Opprtunity") / 100.00;
			wPhc = json.getDouble("Slider_PhoneCall") / 100.00;
			range = json.getInt("TextField_MaxPerson");
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		whereStatements.addAll(createWherPersonID(json));
		whereStatements.addAll(createWhereisEmployee(json));
		whereStatements.addAll(createWhereisCompany(json));
		whereStatements.addAll(createWhereisCompanyAndContact(json));
		whereStatements.addAll(createWhereisContact(json));
		whereStatements.addAll(createWhereIgnorePerson(con, json));
		whereStatements.addAll(createWhereTown(con, json));
		whereStatements.addAll(createWhereCountry(con, json));
		whereStatements.addAll(createWhereGroup(con, json));
		whereStatements.addAll(createWhereDateRange(json));

		String where = buildWhereClausel();
		long zstVorher;
		long zstNachher;

		zstVorher = System.nanoTime();
		
		String query = "";
		
		if(startDiff != 0 || endDiff != 0) {
			query = "" +
					"SELECT SUM ( InnerTable.DOC + InnerTable.EML + InnerTable.APP + InnerTable.OPP +InnerTable.PHC) AS Qsum " +
					", SUM(InnerTable.DOC) AS DOC " +
					", SUM(InnerTable.EML) AS EML " +
					", SUM(InnerTable.APP) AS APP " +
					", SUM(InnerTable.OPP) AS OPP " +
					", SUM(InnerTable.PHC) AS PHC " +
					", InnerTable.LinkedPersonID  " +
						"FROM ( SELECT (CASE WHEN OTyp = 1 THEN (COUNT (OTyp))*"+wDoc+" ELSE  0 END ) AS DOC " +
							",(CASE WHEN OTyp = 2 THEN (COUNT (OTyp))*"+wEml+" ELSE  0 END ) AS EML " +
							",(CASE WHEN OTyp = 3 THEN (COUNT (OTyp))*"+wApp+" ELSE  0 END ) AS APP " +
							",(CASE WHEN OTyp = 4 THEN (COUNT (OTyp))*"+wGwop+" ELSE  0 END ) AS OPP " +
							",(CASE WHEN OTyp = 5 THEN (COUNT (OTyp))*"+wPhc+" ELSE  0 END ) AS PHC " +
							", OrigTable.LinkedPersonID " +
							", SUM(OrigTable.GEW) AS GewSumm " +
								"FROM (SELECT OTyp, LinkedPersonID, (CASE WHEN DateDay BETWEEN "+startDay+" AND "+startDayPlus+" THEN ((DateDay-"+startDay+")*"+faktorStart+") " +
										"WHEN DateDay BETWEEN "+endDayPlus+" AND "+endDay+" THEN (("+endDay+"-DateDay)*"+faktorEnd+") ELSE 1 END) AS GEW " +
												"FROM data WHERE "+where+") AS OrigTable " +
								"GROUP BY OTyp , LinkedPersonID, OrigTable.GEW) AS InnerTable " + 
						"GROUP BY InnerTable.LinkedPersonID " +
						"ORDER BY Qsum DESC " +
						"LIMIT 0, "+range+" ";
		}
		else {
			query = "" +
					"SELECT SUM ( InnerTable.DOC + InnerTable.EML + InnerTable.APP + InnerTable.OPP +InnerTable.PHC) AS Qsum " +
					", SUM(InnerTable.DOC) AS DOC " +
					", SUM(InnerTable.EML) AS EML " +
					", SUM(InnerTable.APP) AS APP " +
					", SUM(InnerTable.OPP) AS OPP " +
					", SUM(InnerTable.PHC) AS PHC " +
					", InnerTable.LinkedPersonID  " +
									"FROM ( SELECT (CASE WHEN OTyp = 1 THEN (COUNT (OTyp))*"+wDoc+" ELSE  0 END ) AS DOC " +
												",(CASE WHEN OTyp = 2 THEN (COUNT (OTyp))*"+wEml+" ELSE  0 END ) AS EML " +
												",(CASE WHEN OTyp = 3 THEN (COUNT (OTyp))*"+wApp+" ELSE  0 END ) AS APP " +
												",(CASE WHEN OTyp = 4 THEN (COUNT (OTyp))*"+wGwop+" ELSE  0 END ) AS OPP " +
												",(CASE WHEN OTyp = 5 THEN (COUNT (OTyp))*"+wPhc+" ELSE  0 END ) AS PHC " +
												", LinkedPersonID " +
											"FROM data " + 
											"WHERE " + where +
											"GROUP BY OTyp , LinkedPersonID) AS InnerTable " +
									"GROUP BY InnerTable.LinkedPersonID " +
									"ORDER BY Qsum DESC " +
									"LIMIT 0, "+range+" ";
		}
//		else {
//		
//		query = "" +
//				"SELECT (SUM ( InnerTable.CountType ) * SUM(InnerTable.GewSumm)) AS Qsum, InnerTable.LinkedPersonID " +
//					"FROM ( SELECT (CASE " +
//								"WHEN OTyp = 1 THEN (COUNT (OTyp))*"+wDoc+" " +
//								"WHEN OTyp = 2 THEN (COUNT (OTyp))*"+wEml+" " +
//								"WHEN OTyp = 3 THEN (COUNT (OTyp))*"+wApp+" " +
//								"WHEN OTyp = 4 THEN (COUNT (OTyp))*"+wGwop+" " +
//								"WHEN OTyp = 5 THEN (COUNT (OTyp))*"+wPhc+" END) AS CountType " +
//								",  OrigTable.LinkedPersonID, SUM(OrigTable.GEW) AS GewSumm " +
//							"FROM (SELECT OTyp, LinkedPersonID, (CASE WHEN DateDay BETWEEN "+startDay+" AND "+startDayPlus+" THEN ((DateDay-"+startDay+")*"+faktorStart+") " +
//									"WHEN DateDay BETWEEN "+endDayPlus+" AND "+endDay+" THEN (("+endDay+"-DateDay)*"+faktorEnd+") ELSE 1 END) AS GEW " +
//											"FROM data WHERE "+where+") AS OrigTable " +
//							"GROUP BY OTyp , LinkedPersonID, OrigTable.GEW) AS InnerTable " + 
//					"GROUP BY InnerTable.LinkedPersonID " +
//					"ORDER BY Qsum DESC " +
//					"LIMIT 0, "+range+" ";
//		
//			query = "" +
//					"SELECT SUM ( InnerTable.CountType ) AS Qsum, InnerTable.LinkedPersonID " +
//						"FROM ( SELECT (CASE " +
//									"WHEN OTyp = 1 THEN (COUNT (OTyp))*"+wDoc+" " +
//									"WHEN OTyp = 2 THEN (COUNT (OTyp))*"+wEml+" " +
//									"WHEN OTyp = 3 THEN (COUNT (OTyp))*"+wApp+" " +
//									"WHEN OTyp = 4 THEN (COUNT (OTyp))*"+wGwop+" " +
//									"WHEN OTyp = 5 THEN (COUNT (OTyp))*"+wPhc+" END) AS CountType " +
//									", LinkedPersonID " +
//								"FROM data " +
//								"WHERE " + where +
//								"GROUP BY OTyp , LinkedPersonID) AS InnerTable " + 
//						"GROUP BY InnerTable.LinkedPersonID " +
//						"ORDER BY Qsum DESC " +
//						"LIMIT 0, "+range+" ";
//		}
	
		JSONObject result = new JSONObject();
		try {
			Statement statement = con.createStatement();
			ResultSet rsSET = statement.executeQuery(query);
			while (rsSET.next()) {
				JSONArray  container = new JSONArray();
				container.put(rsSET.getInt(1));
				container.put(rsSET.getInt(2));
				container.put(rsSET.getInt(3));
				container.put(rsSET.getInt(4));
				container.put(rsSET.getInt(5));
				container.put(rsSET.getInt(6));
				result.put(rsSET.getString(7), container);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
//		JSONObject result = new JSONObject();
//		try {
//			Statement statement = con.createStatement();
//			ResultSet rsSET = statement.executeQuery(query);
//			while (rsSET.next()) {
//				result.put(rsSET.getString(2), rsSET.getInt(1));
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} catch (JSONException e) {
//			e.printStackTrace();
//		}
		
		// Aufruf lange dauernder Prozesse

		zstNachher = System.nanoTime();
		System.out.println("Zeit benötigt: " + ((zstNachher - zstVorher)/1000) + " us - " + ((zstNachher - zstVorher)/1000/1000) + " ms");
		
		return result;
	}

	private String buildWhereClausel() {
		String result = "";
		for (int i = 0; i < whereStatements.size(); ++i) {
			if (i == 0) {
				result += " " + whereStatements.get(i) + " ";
			} else {
				result += " AND " + whereStatements.get(i) + " ";
			}
		}
		return result;
	}

	private ArrayList<String> createWherPersonID(JSONObject json) {
		ArrayList<String> container = new ArrayList<String>();
		try {
			container.add("userID = "+json.getString("UserID"));
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return container;
	}
	
	private ArrayList<String> createWhereDateRange(JSONObject json) {
		ArrayList<String> container = new ArrayList<String>();
		try {
			DateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

			String start = json.getString("DateField_Start");
			String end = json.getString("DateField_End");

			Date dateStart = formatter.parse(start);
			Date dateEnd = formatter.parse(end);
			Date dateBeginn = formatter.parse("MON JAN 01 00:00:00 CET 1990");

			long startFinal = getDateDiff(dateBeginn, dateStart, TimeUnit.DAYS);
			long endFinal = getDateDiff(dateBeginn, dateEnd, TimeUnit.DAYS);

			whereStatements.add("(DateDay >= " + startFinal + " AND DateDay <= " + endFinal + ")");
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return container;
	}

	private ArrayList<String> createWhereisEmployee(JSONObject json) {
		ArrayList<String> container = new ArrayList<String>();
		try {
			if (json.getBoolean("CheckBox_isEmployee") == true) {
				if (json.getInt("comboBox_isEmployee") == 1) {
					container.add("isEmployee = 1");
				} else if (json.getInt("comboBox_isEmployee") == 0) {
					container.add("isEmployee = 0");
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return container;
	}

	private ArrayList<String> createWhereisCompany(JSONObject json) {
		ArrayList<String> container = new ArrayList<String>();
		try {
			if (json.getBoolean("CheckBox_isCompany") == true) {
				container.add("isCompany = 1");
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return container;
	}

	private ArrayList<String> createWhereisCompanyAndContact(JSONObject json) {
		ArrayList<String> container = new ArrayList<String>();
		try {
			if (json.getBoolean("CheckBox_isCompanyAndContact") == true) {
				container.add("(isContact = 1 AND isCompany = 1)");
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return container;
	}

	private ArrayList<String> createWhereisContact(JSONObject json) {
		ArrayList<String> container = new ArrayList<String>();
		try {
			if (json.getBoolean("CheckBox_isContact") == true) {
				container.add("isContact = 1");
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return container;
	}

	private ArrayList<String> createWhereIgnorePerson(Connection con, JSONObject json) {
		ArrayList<String> container = new ArrayList<String>();
		try {
			if (json.getBoolean("CheckBox_IgnorePerson") == true) {
				String name = json.getString("TextField_PersonIgnore");
				int personID = 0;
				try {
					Statement statement = con.createStatement();
					String query = "SELECT id FROM ClientUser WHERE name LIKE '" + name + "'";
					ResultSet rsSET = statement.executeQuery(query);
					while (rsSET.next()) {
						personID = rsSET.getInt(1);
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}

				if (personID != 0) {
					container.add("LinkedPersonID != " + personID);
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return container;
	}

	private ArrayList<String> createWhereTown(Connection con, JSONObject json) {
		ArrayList<String> container = new ArrayList<String>();
		try {
			if (json.getBoolean("CheckBox_Town") == true) {
				String name = json.getString("TextField_Town");
				int townID = -1;
				try {
					Statement statement = con.createStatement();
					String query = "SELECT id FROM D_Town WHERE name LIKE '" + name + "'";
					ResultSet rsSET = statement.executeQuery(query);
					while (rsSET.next()) {
						townID = rsSET.getInt(1);
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				if (townID >= 0) {
					if (json.getInt("comboBox_Town") == 1) {
						container.add("Town = " + townID);
					} else if (json.getInt("comboBox_Town") == 0) {
						container.add("Town != " + townID);
					}
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return container;
	}

	private ArrayList<String> createWhereCountry(Connection con, JSONObject json) {
		ArrayList<String> container = new ArrayList<String>();
		try {
			if (json.getBoolean("CheckBox_Country") == true) {
				String name = json.getString("TextField_Country");
				int countryID = -1;
				try {
					Statement statement = con.createStatement();
					String query = "SELECT id FROM D_Country WHERE name LIKE '" + name + "'";
					ResultSet rsSET = statement.executeQuery(query);
					while (rsSET.next()) {
						countryID = rsSET.getInt(1);
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				if (countryID >= 0) {
					if (json.getInt("comboBox_Country") == 1) {
						container.add("Country = " + countryID);
					} else if (json.getInt("comboBox_Town") == 0) {
						container.add("Country != " + countryID);
					}
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return container;
	}

	private ArrayList<String> createWhereGroup(Connection con, JSONObject json) {
		ArrayList<String> container = new ArrayList<String>();
		try {
			if (json.getBoolean("CheckBox_Group") == true) {
				if (!json.getString("OptionGroup_Group").equals("[]")) {
					String value = json.getString("OptionGroup_Group").replace("[", "");
					value = value.replace("]", "");
					String[] values = value.split(",");
					for (int i = 0; i < values.length; ++i) {
						String name = values[i];
						int groupID = 0;
						try {
							Statement statement = con.createStatement();
							String query = "SELECT id FROM D_SysUserGroup WHERE name LIKE '" + name + "'";
							ResultSet rsSET = statement.executeQuery(query);
							while (rsSET.next()) {
								groupID = rsSET.getInt(1);
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}
						if (groupID != 0) {
							container.add("GroupID != " + groupID);
						}
					}
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return container;
	}

	public static long getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
		long diffInMillies = date2.getTime() - date1.getTime();
		return timeUnit.convert(diffInMillies, TimeUnit.MILLISECONDS);
	}
}
