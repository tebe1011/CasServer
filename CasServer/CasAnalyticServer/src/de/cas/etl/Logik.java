package de.cas.etl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Logik {

	public Logik() {

	}

	public void buildQuery(Connection con, JSONObject json) {

		ArrayList<String> whereStatements = new ArrayList<String>();

		try {
			if (json.getBoolean("CheckBox_isEmployee") == true) {
				if (json.getInt("comboBox_isEmployee") == 1) {
					whereStatements.add("isEmployee = 1");
				} else if (json.getInt("comboBox_isEmployee") == 0) {
					whereStatements.add("isEmployee = 0");
				}
			}

			if (json.getBoolean("CheckBox_isCompany") == true) {
				whereStatements.add("isCompany = 1");
			}

			if (json.getBoolean("CheckBox_isCompanyAndContact") == true) {
				whereStatements.add("(isContact = 1 AND isCompany = 1)");
			}

			if (json.getBoolean("CheckBox_isContact") == true) {
				whereStatements.add("isContact = 1");
			}

			if (json.getBoolean("CheckBox_IgnorePerson") == true) {

				String name = json.getString("TextField_PersonIgnore");
				int personID = 0;

				try {
					Statement statement = con.createStatement();
					String query = "SELECT id FROM D_SysUser WHERE name LIKE '" + name + "'";
					ResultSet rsSET = statement.executeQuery(query);
					while (rsSET.next()) {
						personID = rsSET.getInt(1);
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}

				if (personID != 0) {
					whereStatements.add("LinkedPersonID != " + personID);
				}
			}

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
						whereStatements.add("Town = " + townID);
					} else if (json.getInt("comboBox_Town") == 0) {
						whereStatements.add("Town != " + townID);
					}
				}
			}

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
						whereStatements.add("Country = " + countryID);
					} else if (json.getInt("comboBox_Town") == 0) {
						whereStatements.add("Country != " + countryID);
					}
				}
			}

			if (json.getBoolean("CheckBox_Group") == true) {
				if(!json.getString("OptionGroup_Group").equals("[]")) {
					String value = json.getString("OptionGroup_Group").replace("[", "");
					value = value.replace("]", "");
					String[] values =  value.split(",");
					
					for(int i = 0; i < values.length; ++i) {
						
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
						
						if(groupID != 0) {
							whereStatements.add("GroupID != " + groupID);
						}						
					}
				}
			}
			  
			try{
				DateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
			    
				String start = json.getString("DateField_Start");
				String end = json.getString("DateField_End");
				
			    Date dateStart = formatter.parse(start);
			    Date dateEnd = formatter.parse(end);
			}
			catch(ParseException ex2){
				ex2.printStackTrace();
			}

		} catch (JSONException e) {
			e.printStackTrace();
		}

		// String query =
		// "SELECT COUNT(LinkedPersonID), LinkedPersonID FROM DATA WHERE userID = 10 GROUP BY LinkedPersonID";

	}
}
