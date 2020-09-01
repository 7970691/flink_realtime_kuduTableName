package com.xm4399.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;


public class ListAllSubTableName {
    //获取所有分表名
    public  ArrayList<String> listAllSmallTableName(String address, String username, String password, String dbName, String tableName){
        ArrayList<String> allSmallTableList = new ArrayList<String>() ;
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            con = DriverManager.getConnection("jdbc:mysql://" + address +"/" + dbName,
                    username, password);

           stmt = con.createStatement();
            //获取以tableName_开头的 后面跟着0-100  的所有表的表名
            String sql = "select table_name from information_schema.tables where table_name REGEXP '"  + tableName + "_" + "[0-9]{1,3}';";
            res = stmt.executeQuery(sql);
            while (res.next()) {
                String smallTableName = res.getString(1);
                System.out.println("将表  " + smallTableName +"  加入集合>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                allSmallTableList.add(smallTableName);

            }
            res.close();
            stmt.close();
            con.close();
            return allSmallTableList;
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            res = null;
            stmt = null;
            con = null;
        }
        return allSmallTableList;
    }

    }

