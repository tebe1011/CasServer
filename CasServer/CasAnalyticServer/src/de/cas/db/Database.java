package de.cas.db;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;

public class Database {
	
	private DataSource dataSource;
	private SchemaBuilder schemaBuilder;
	public static Connection con;

	public Database() throws SQLException {

		this.schemaBuilder = new SchemaBuilder();
		this.dataSource = JdbcConnectionPool.create("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "user", "password");
		this.con = dataSource.getConnection();
		System.out.println("H2-DATABASE: STARTED.");
		schemaBuilder.createSchema(dataSource);
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
