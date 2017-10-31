package com.sihuatech.sqm.spark.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBConnection {
	private static String username;
	private static String url;
	private static String password;

	public static Connection getConnection() {
		getConfig();
		Connection con = null;
		try {
			System.out.println("==="+url);
			System.out.println("==="+username);
			System.out.println("==="+password);
			con = DriverManager.getConnection(url, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return con;
	}

	public static void closeDB(Connection con, Statement stmt, ResultSet rs) {
		try {
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			if (con != null)
				con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void getConfig() {
		url = PropHelper.getProperty("jdbc.url");
		username = PropHelper.getProperty("jdbc.username");
		password = PropHelper.getProperty("jdbc.password");
	}
}
