package de.cas.db;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class SchemaBuilder {


	public SchemaBuilder() {

	}
	
	public void createSchema(DataSource ds) throws SQLException {
		Connection conn = ds.getConnection();
		conn.createStatement().executeUpdate("CREATE TABLE data ( " +
				"userID SMALLINT NOT NULL, " +
				"Date INT NOT NULL, " +
				"OTyp TINYINT NOT NULL, " +
				"LinkedPersonID SMALLINT NOT NULL, " +
				"isCompany TINYINT NOT NULL, " +
				"isContact TINYINT NOT NULL, " +
				"isEmployee TINYINT NOT NULL," +
				"Town SMALLINT NOT NULL, " +
				"Country SMALLINT NOT NULL, " +
				"GroupID SMALLINT NOT NULL, " +
				" )");
		
		conn.createStatement().executeUpdate("CREATE TABLE D_Town ( " +
				"id SMALLINT NOT NULL, " +
				"name VARCHAR(45) NOT NULL, " +
				" )");
		
		conn.createStatement().executeUpdate("CREATE TABLE D_Country ( " +
				"id SMALLINT NOT NULL, " +
				"name VARCHAR(45) NOT NULL, " +
				" )");
		
		conn.createStatement().executeUpdate("CREATE TABLE D_SysUser ( " +
				"id SMALLINT NOT NULL, " +
				"name VARCHAR(40) NOT NULL, " +
				" )");
		
		conn.createStatement().executeUpdate("CREATE TABLE D_SysUserGroup ( " +
				"id SMALLINT NOT NULL, " +
				"name VARCHAR(40) NOT NULL, " +
				" )");
		
		conn.close();
		System.out.println("H2-DATABASE: SCHEMA CREATED.");
	}
}
