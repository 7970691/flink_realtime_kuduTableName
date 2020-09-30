package com.xm4399.run;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.xm4399.tt.MyStringClass;
import com.xm4399.tt.MyStringDebeziumDeserializeSchema;
import com.xm4399.util.ConfUtil;
import com.xm4399.util.JDBCOnlineUtil;
import com.xm4399.util.JDBCUtil;
import com.xm4399.util.KuduSink;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.util.HashMap;

/**
 * @Auther: czk
 * @Date: 2020/9/25
 * @Description:
 */
public class StringBinlogTest {
    public static void main(String[] args) throws Exception {
        /*String jobID = args[0];
        String firstOrFromCheckPoint = args[1];
        String[] conInfoArr = new JDBCUtil().getConfInfoArr(jobID);
        String address = conInfoArr[0];
        String username = conInfoArr[1];
        String password = conInfoArr[2];
        String dbName = conInfoArr[3];
        String tableName = conInfoArr[4];
        String isSubTable = conInfoArr[6];
        String kuduTableName = conInfoArr[8];
        mysql2Kudu(address, username, password, dbName, tableName, isSubTable, kuduTableName, jobID, firstOrFromCheckPoint);*/
        mysql2Kudu("localhost:3306", "root", "a5515458", "chenzhikun",
                "sub_1", "false", "kuduTableName","111","FirstFlinkJob");
    }

    public static void mysql2Kudu(String address, String username, String password, String dbName, String tableName,
                                  String isSubTable, String kuduTableName, String jobID, String firstOrFromCheckPoint) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRestartStrategy(RestartStrategies.noRestart());
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart( 2147483647,5));
       /* env.enableCheckpointing( 5 * 60 * 1000);
        String checkPointDir = new ConfUtil().getValue("checkpointDir");
        env.setStateBackend(new RocksDBStateBackend(checkPointDir, true));
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setTolerableCheckpointFailureNumber(30);
        config.setCheckpointTimeout(20 * 60* 1000);*/

        String[] tableArr = null;
        if ("true".equals(isSubTable)){
            tableArr =  new JDBCOnlineUtil().listAllSubTableName(address, username, password, dbName, tableName).toArray(new String[]{});
        } else {
            tableArr = new String[]{dbName + "." + tableName};
        }
        String[] addressArr = address.split(":");
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(addressArr[0])
                .port(Integer.parseInt(addressArr[1]))
                .tableList(tableArr)
                .username(username)
                .password(password)
                .deserializer( new StringDebeziumDeserializationSchema())
                .build();
        System.out.println("---------------------------------> start");

        env.addSource(sourceFunction).map(record ->{
            System.out.println(record.toString());
            return record;
        });

                //.addSink(new KuduSink(tableName, isSubTable, kuduTableName)).setParallelism(5);
        /*try {
            //firstOrFromCheckPoint的值为FirstFlinkJob 或 StartFlinkJobFromCheckPoint
            new JDBCUtil().updateJobState(jobID, "2_"+firstOrFromCheckPoint);
            env.execute("flink-cdc-" + jobID);
        }catch (Exception e){
            new JDBCUtil().updateJobState(jobID, "-1_"+firstOrFromCheckPoint);
            new JDBCUtil().insertErroeInfo(jobID, firstOrFromCheckPoint, "" );
            e.printStackTrace();
        }*/
    }
}
