package com.xm4399.util;

import java.sql.*;
import java.util.ArrayList;

/**
 * @Auther: czk
 * @Date: 2020/9/21
 * @Description:
 */
public class JDBCOnlineUtil {

    // 获取所有分表名
    public ArrayList<String> listAllSubTableName(String address, String username, String password, String  dbName, String tableName){
        ArrayList<String> allSubTableList = new ArrayList<String>() ;
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            con = getConnection(address,username,password,dbName);
            stmt = con.createStatement();
            //获取以tableName_开头的 后面跟着0-100  的所有表的表名
            String sql = "select table_name from information_schema.tables where table_name REGEXP '"  + tableName + "_" + "[0-9]{1,3}';";
            res = stmt.executeQuery(sql);
            while (res.next()) {
                String subTableName = res.getString(1);
                String dbNameAndTableName = dbName + "." + subTableName;
                allSubTableList.add(dbNameAndTableName);
            }
            return allSubTableList;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(res,stmt,con);
        }
        return allSubTableList;
    }

    public  Connection getConnection (String address, String username, String password,String dbName){
        Connection connection = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://" + address + "/" + dbName, username, password);
            //connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/chenzhikun", "canal", "canal");
            return connection;
        } catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    public  void close(ResultSet res, Statement stmt, Connection con ) {
        try {
            if (res != null) {
                res.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            res = null;
            stmt = null;
            con = null;
        }
    }


}
