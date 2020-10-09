package com.xm4399.run;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.xm4399.util.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * @Auther: czk
 * @Date: 2020/9/15
 * @Description:
 */
public class MySqlBinlogSourceExample {

    public static void main(String[] args) throws Exception {
        String jobID = args[0];
        String firstOrFromCheckPoint = args[1];
        int parallelismCount = Integer.parseInt(args[2]);
        String[] conInfoArr = new JDBCUtil().getConfInfoArr(jobID);
        String address = conInfoArr[0];
        String username = conInfoArr[1];
        String password = conInfoArr[2];
        String dbName = conInfoArr[3];
        String tableName = conInfoArr[4];
        String isSubTable = conInfoArr[6];
        String kuduTableName = conInfoArr[7];
        mysql2Kudu(address, username, password, dbName, tableName, isSubTable, kuduTableName, jobID, firstOrFromCheckPoint, parallelismCount);
        /*mysql2Kudu("10.0.0.92:3310", "cnbbsReadonly", "LLKFN*k241235", "4399_cnbbs",
                "thread_image_like_user_0", "false", "kuduTableName","111","FirstFlinkJob");*/
    }

    public static void mysql2Kudu(String address, String username, String password, String dbName, String tableName, String isSubTable,
                                  String kuduTableName, String jobID, String firstOrFromCheckPoint, int parallelismCount) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing( 20 * 60 * 1000);
        String checkPointDir = new ConfUtil().getValue("checkpointDir");
        env.setStateBackend(new RocksDBStateBackend(checkPointDir, true));
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        config.setTolerableCheckpointFailureNumber(15);
        config.setCheckpointTimeout(60 * 60* 1000);

        String[] tableArr = null;
        if ("true".equals(isSubTable)){
            tableArr =  new JDBCOnlineUtil().listAllSubTableName(address, username, password, dbName, tableName).toArray(new String[]{});
        } else {
            tableArr = new String[]{dbName + "." + tableName};
        }

        String[] addressArr = address.split(":");
        SourceFunction<MyStringClass> sourceFunction = MySQLSource.<MyStringClass>builder()
                .hostname(addressArr[0])
                .port(Integer.parseInt(addressArr[1]))
                .tableList(tableArr)
                .username(username)
                .password(password)
                .deserializer(new MyStringDebeziumDeserializeSchema())
                //.debeziumProperties(props)
                .build();

        env.addSource(sourceFunction)
                .keyBy(row -> {
                    StringBuffer sb = new StringBuffer();
                    HashMap<String, String> pkMap = row.getPrikey();
                    for (String onePk : pkMap.values()) {
                        sb.append(onePk);
                    }
                    return sb.toString();
                })
                .addSink(new KuduSink(tableName, isSubTable, kuduTableName, jobID)).setParallelism(parallelismCount);

        try {
            //firstOrFromCheckPoint的值为FirstFlinkJob 或 StartFlinkJobFromCheckPoint
            new JDBCUtil().updateJobState(jobID, "2_"+firstOrFromCheckPoint);
            env.execute("flink-cdc-" + jobID);
        }catch (Exception e){
            new JDBCUtil().updateJobState(jobID, "-1_"+firstOrFromCheckPoint);
            new JDBCUtil().insertErroeInfo(jobID, firstOrFromCheckPoint, "" );
            final Logger logger = LoggerFactory.getLogger(MySqlBinlogSourceExample.class);
            logger.error("exception info :", e);
            System.out.println(e.toString());
        }


    }
}
