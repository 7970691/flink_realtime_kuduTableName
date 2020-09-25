package com.xm4399.jdbc;

import java.sql.*;

/**
 * @Auther: czk
 * @Date: 2020/9/21
 * @Description:
 */
public class InsertTest {
    static int i =5;
    public static void main(String[] args) {
        //System.out.println(System.currentTimeMillis());
        s();
        System.out.println(i);
       //insertTest(args[0],Integer.parseInt(args[1]),Integer.parseInt(args[2]));
       // insertTest("fen_2",20001,30000);
        //insertTest("fen_3",20001,30000);
       // updateJobState(args[0]);
    }
    public static void  s () {
        i = 6;
    }

    public static void insertTest(String tableName, int before, int after)  {
        Connection connection = null;
        PreparedStatement pst =null ;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://" + "10.0.0.211:3307" + "/" + "chenzhikun_test", "gprp", "gprp@@4399");
            //connection = DriverManager.getConnection("jdbc:mysql://" + "localhost:3306" + "/" + "chenzhikun", "root", "a5515458");
            int i =before;
            while (i < after){
                i++;
                String sql = "insert into " +  tableName + " (id, username) values(?,?)";
                pst = connection.prepareStatement(sql);
                pst.setInt(1,i);
                pst.setString(2,"xxx-" + i);
                pst.executeUpdate();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                pst.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                pst = null;
                connection = null;
            }
        }
    }


    public static void updateJobState(String ID)  {
        Connection connection = null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://" + "10.0.0.211:3307" + "/" + "chenzhikun_test", "gprp", "gprp@@4399");
            stmt = connection.createStatement();
            int i =0;
            while (i<1000){
                String sql =  "update fen_1 set username = \"" +"xxx-" + i  +"\"  where id = " + ID ;
                stmt.executeUpdate(sql);
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                stmt.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                stmt = null;
                connection = null;
            }
        }
    }


}
