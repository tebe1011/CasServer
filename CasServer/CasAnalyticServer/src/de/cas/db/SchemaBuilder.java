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
				"DateDay INT NOT NULL, " +
				"OTyp TINYINT NOT NULL, " +
				"LinkedPersonID SMALLINT NOT NULL, " +
				"isCompany TINYINT, " +
				"isContact TINYINT, " +
				"isEmployee TINYINT," +
				"Town SMALLINT, " +
				"Country SMALLINT, " +
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
		
		conn.createStatement().executeUpdate("CREATE TABLE ClientUser ( " +
				"id SMALLINT NOT NULL, " +
				"name VARCHAR(40) NOT NULL, " +
				" )");
		
		conn.createStatement().executeUpdate("CREATE TABLE D_SysUserGroup ( " +
				"id SMALLINT NOT NULL, " +
				"name VARCHAR(40) NOT NULL, " +
				" )");
		
		conn.createStatement().executeUpdate("CREATE TABLE GroupRelation ( " +
				"oid SMALLINT NOT NULL, " +
				"gid SMALLINT NOT NULL, " +
				" )");
		
		conn.close();
		System.out.println("H2-DATABASE: SCHEMA CREATED.");
	}
}
