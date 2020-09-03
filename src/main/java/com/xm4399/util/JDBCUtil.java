package com.xm4399.util;

import java.sql.*;

/**
 * @Auther: czk
 * @Date: 2020/8/13
 * @Description:
 */
public class JDBCUtil {
    // 根据jobID获取数据同步任务的配置参数
    public String[] getConfInfoArr(String jobID){
        String[] confInfoArr = new String[10];
        Connection con = null ;
        Statement stmt =null;
        ResultSet res = null;
        try {
            con = getConnection();
            stmt = con.createStatement();
            String sql = "select * from  data_syn_status where job_id = "  + jobID + ";" ;
            res = stmt.executeQuery(sql);
            while (res.next()) {
                String address = res.getString(3);
                String username = res.getString(4);
                String password = res.getString(5);
                String dbName = res.getString(6);
                String tableName = res.getString(7);
                String fields = res.getString(8);
                String is_subtable = res.getString(9);
                String topic = res.getString(10);
                String kudu_table_name = res.getString(11);
                String mode = res.getString(12);
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
            return confInfoArr;
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            close(res, stmt, con);
        }

        return  confInfoArr;
    }

    // 更改任务运行状态
    public  void updateJobState(String jobID, String jobState )  {
        Connection connection = null;
        Statement stmt = null;
        try {
            connection = getConnection();
            stmt = connection.createStatement();
            String sql =  "update data_syn_status set job_state = \"" +jobState +"\"  where job_id = " + jobID ;
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            close(null,stmt,connection);
        }
    }


    public  Connection getConnection (){
        Connection connection = null;
        String address = new ConfUtil().getValue("address");
        String username = new ConfUtil().getValue("username");
        String password = new ConfUtil().getValue("password");
        String dbName = new ConfUtil().getValue("dbName");
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://" + address + "/chenzhikun_test", username, password);
            return connection;
        } catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    /** 关闭链接,释放资源 */
    public  void close(ResultSet res,Statement stmt,Connection con ) {
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

   /* // flink任务running,更新mysql状态
    public static void updateRunningFlinkRealtimeStatus(String jobID )  {
        Connection connection = null;
        Statement stmt = null;
        try {
            connection = getConnection();
            stmt = connection.createStatement();
            String sql = "update data_syn_status set flink_realtime_status = 2 where job_id = " + jobID + ";";
            stmt.executeUpdate(sql);
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            close(connection);
        }
    }

    // flink任务异常,更新mysql状态
    public static void updateExceptionFlinkRealtimeStatus(String jobID )  {
        Connection connection = null;
        Statement stmt = null;
        try {
            connection = getConnection();
            stmt = connection.createStatement();
            String sql = "update data_syn_status set flink_realtime_status =-1 where job_id = " + jobID + ";";
            stmt.executeUpdate(sql);
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            close(connection);
        }
    }*/

}
