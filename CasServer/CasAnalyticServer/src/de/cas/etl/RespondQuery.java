package de.cas.etl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class RespondQuery {

	public RespondQuery () {
		
	}
	
	public ArrayList<String[]> getRespond(Connection con) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			Statement statement = con.createStatement();
			String query = "SELECT COUNT(LinkedPersonID), LinkedPersonID FROM DATA WHERE userID = 10 GROUP BY LinkedPersonID";
			ResultSet rsSET = statement.executeQuery(query);
			while(rsSET.next()) {
				result.add(new String[]{rsSET.getString(1), rsSET.getString(2)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	} 
}
