package db;

import java.sql.*;

public class DB {
	final static String db_driver = "com.ibm.db2.jcc.DB2Driver";
	//final static String db_url = "jdbc:db2://9.110.83.168:50000/PMQNEW";
	final static String db_url = "jdbc:db2://9.125.73.131:50000/PMQNEW";
	final static String db_user = "db2inst1";
	final static String db_pwd = "db2inst1";
	
	/**
	 * @return connection
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public static Connection getConnection() {
		Connection conn = null;
		try {
			Class.forName(db_driver);
			conn = DriverManager.getConnection(db_url,db_user,db_pwd);  
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	public static PreparedStatement prepare(Connection conn,  String sql) {
		PreparedStatement pstmt = null; 
		try {
			if(conn != null) {
				pstmt = conn.prepareStatement(sql);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return pstmt;
	}
	
	public static Statement getStatement(Connection conn)  {
		Statement stmt = null; 
		try {
			if(conn != null) {
				stmt = conn.createStatement();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return stmt;
	}
	
	public static ResultSet getResultSet(Statement stmt, String sql) {
		ResultSet rs = null;
		try {
			if(stmt != null) {
				rs = stmt.executeQuery(sql);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	public static void executeUpdate(Statement stmt, String sql) {
		try {
			if(stmt != null) {
				stmt.executeUpdate(sql);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void close(Connection conn) {
		try {
			if(conn != null) {
				conn.close();
				conn = null;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void close(Statement stmt) {
		try {
			if(stmt != null) {
				stmt.close();
				stmt = null;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void close(ResultSet rs) {
		try {
			if(rs != null) {
				rs.close();
				rs = null;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
