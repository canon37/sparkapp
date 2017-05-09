package com.zuipin.sparkapp.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Title: DBHelper
 * @Package: com.zuipin.webapp.sparkapp
 * @author: zengxinchao
 * @date: 2016年11月22日 上午10:48:40
 * @Description: DBHelper
 */
public class DBHelper {
	public static Connection createConnection() throws SQLException, ClassNotFoundException {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://168.168.2.221/cra_test?useUnicode=true&autoReconnect=true&characterEncoding=UTF-8";
			String username = "root";
			String password = "123456";
			return DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException e) {
			System.out.println("找不到mysql驱动程序类 ，加载驱动失败！");
			throw e;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public static Connection createPrestoConnection() throws SQLException, ClassNotFoundException {
		try {
			Class.forName("com.facebook.presto.jdbc.PrestoDriver");
			String url = "jdbc:presto://168.168.2.221:28080/hive/cra_test";
			String username = "root";
			String password = "";
			return DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException e) {
			System.out.println("找不到presto驱动程序类 ，加载驱动失败！");
			throw e;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}
}
