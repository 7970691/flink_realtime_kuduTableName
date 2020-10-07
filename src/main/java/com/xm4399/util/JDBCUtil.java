package com.xm4399.util;

import java.sql.*;
import java.util.ArrayList;

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
                String isSubtable = res.getString(9);
                String kuduTableName = res.getString(10);
                String mode = res.getString(11);
                confInfoArr[0] = address;
                confInfoArr[1] = username;
                confInfoArr[2] = password;
                confInfoArr[3] = dbName;
                confInfoArr[4] = tableName;
                confInfoArr[5] = fields;
                confInfoArr[6] = isSubtable;
                //confInfoArr[7] = topic;
                confInfoArr[7] = kuduTableName;
                confInfoArr[8] = mode;
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

    //写入报错信息
    public  void insertErroeInfo(String jobID, String jobPart, String errorMsg)  {
        Connection connection = null;
        PreparedStatement pst =null ;

        try {
            connection = getConnection();
            String sql = "insert into error_log (job_id, job_part, error_msg) values(?,?,?)";
            pst = connection.prepareStatement(sql);
            int  jobIDNum = Integer.parseInt(jobID);
            pst.setInt(1,jobIDNum);
            pst.setString(2,jobPart);
            pst.setString(3,errorMsg);
            pst.executeUpdate();
            pst.close();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            close2(pst, connection);
        }
    }

    // 记录 checkpoint 信息
    public  void insertCheckPointInfo(String jobID, String flinkJobID, String checkPointPath)  {
        Connection connection = null;
        PreparedStatement pst =null ;
        try {
            connection = getConnection();
            String sql = "insert into checkpoint_record (job_id, flink_job_id, checkpoint_path) values(?,?,?)";
            pst = connection.prepareStatement(sql);
            int jobId = Integer.parseInt(jobID);
            pst.setInt(1,jobId);
            pst.setString(2,flinkJobID);
            pst.setString(3,checkPointPath);
            pst.executeUpdate();
            pst.close();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            close2(pst, connection);
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
            connection = DriverManager.getConnection("jdbc:mysql://" + address + "/" + dbName, username, password);
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

    public  void close2(PreparedStatement pst,Connection con ) {
        try {
            if (pst != null) {
                pst.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            pst = null;
            con = null;
        }
    }
}
