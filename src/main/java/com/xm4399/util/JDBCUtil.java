package com.xm4399.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @Auther: czk
 * @Date: 2020/8/13
 * @Description:
 */
public class JDBCUtil {
    // 根据jobID获取数据同步任务的配置参数
    public static String[] getConfInfoArr(String jobID){
        String[] confInfoArr = new String[10];
        Connection con = null ;
        Statement stmt ;
        try {
            con = getConnection();
            stmt = con.createStatement();

            // mysql字段名为: 1.job_id   2.address   3.username   4.password    5.db_name    6.table_name
            //  7. fields   8.is_subtable   9.topic   10.kudu_table_name    11.mode
            // 12. kudu_table_status  13. canal_instance_status 14.full_pull_status
            String sql = "select * from  data_syn_status where job_id = "  + jobID + ";" ;
            ResultSet res = stmt.executeQuery(sql);
            while (res.next()) {
                String address = res.getString(2);
                String username = res.getString(3);
                String password = res.getString(4);
                String dbName = res.getString(5);
                String tableName = res.getString(6);
                String fields = res.getString(7);
                String is_subtable = res.getString(8);
                String topic = res.getString(9);
                String kudu_table_name = res.getString(10);
                String mode = res.getString(11);
                confInfoArr[0] = address;
                confInfoArr[1] = username;
                confInfoArr[2] = password;
                confInfoArr[3] = dbName;
                confInfoArr[4] = tableName;
                confInfoArr[5] = fields;
                confInfoArr[6] = is_subtable;
                confInfoArr[7] = topic;
                confInfoArr[8] = kudu_table_name;
                confInfoArr[9] = mode;
            }
            res.close();
            stmt.close();
            return confInfoArr;
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            close(con);
        }

        return  confInfoArr;
    }

    public static Connection getConnection (){
        Connection connection = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://10.0.0.211:3307/chenzhikun_test", "gprp", "gprp@@4399");
            //connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/chenzhikun", "canal", "canal");
            return connection;
        } catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    public  static void close (Connection connection){
        try {
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            //保证资源一定会被释放
            connection = null;
        }
    }

}
