package io.redash.queryrunner.athena;

import java.sql.*;
import java.util.Properties;
import com.amazonaws.athena.jdbc.AthenaDriver;

public class AthenaJDBCConnector {

	Properties info;
	String url;

	public AthenaJDBCConnector(Properties info, String url) {
		this.info = info;
		this.url = url;
	}

	public Connection getConnection() {		
		try {
			Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
			Connection conn = DriverManager.getConnection(this.url, this.info);
			return conn;
		} catch (Exception ex) {
			ex.printStackTrace();	
		}

		return null;
	}

}