package de.cas.db;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;

import de.cas.etl.Load;

public class Database {
	
	private DataSource dataSource;
	private SchemaBuilder schemaBuilder;
	public static Connection con;
	private Load loader;

	public Database() throws SQLException {

		this.schemaBuilder = new SchemaBuilder();
		this.dataSource = JdbcConnectionPool.create("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "user", "password");
		this.con = dataSource.getConnection();
		System.out.println("H2-DATABASE: STARTED.");
		schemaBuilder.createSchema(dataSource);
		this.loader = new Load();
		
		System.out.println("LOAD STARTED");
		
		loader.loadMainData(Database.con);
		loader.loadTownData(Database.con);
		loader.loadCountryData(Database.con);
		loader.loadSysUserData(Database.con);
		loader.loadSysUserGroupData(Database.con);
		loader.loadClientUserData(Database.con);
		
		loader.createIndexPersonID(Database.con);
		loader.createIndexDateDay(Database.con);
		
		System.out.println("LOAD ENDED");
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public SchemaBuilder getSchemaBuilder() {
		return schemaBuilder;
	}

	public void setSchemaBuilder(SchemaBuilder schemaBuilder) {
		this.schemaBuilder = schemaBuilder;
	}

	public Connection getCon() {
		return con;
	}

	public void setCon(Connection con) {
		this.con = con;
	}
}
