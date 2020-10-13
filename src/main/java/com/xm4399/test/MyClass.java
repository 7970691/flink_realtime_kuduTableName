package com.xm4399.test;

import com.xm4399.util.ConfUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.RowKind;
import org.apache.kafka.connect.data.Struct;

import java.io.Serializable;
import java.util.HashMap;


/**
 * @Auther: czk
 * @Date: 2020/9/17
 * @Description:
 */
public class MyClass  {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30001);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        String name = "myhive";
        String defaultDatabase = "flink";
        String hiveConfDir = "G:\\=Flink SQL开发文件"; // a local path
        String version = "1.1.0";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);

        tEnv.registerCatalog("myhive", hive);
        tEnv.useCatalog("myhive");

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("drop table kafkaTable22");
        tEnv.executeSql("CREATE TABLE czkkafka_cdc_test (\n" +
                "      id INT,\n" +
                "       username STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'flink_realtime_test',\n" +
                "    'properties.bootstrap.servers'     = '10.0.0.194:9092,10.0.0.195:9092,10.0.0.199:9092',\n" +
                "    'properties.group.id' = 'test1'," +
                "    'format'     = 'changelog-json',\n" +
                "    'scan.startup.mode'     = 'earliest-offset'\n" +
                ")");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        String hiveSql = "CREATE  TABLE  cdchive_cdc_test (\n" +
                "  id INT,\n" +
                "  username STRING" +
                ")  " +
                "stored as PARQUET " +
                "TBLPROPERTIES (\n" +
                "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
                "  'sink.partition-commit.delay'='5 s',\n" +
                "  'sink.partition-commit.trigger'='partition-time',\n" +
//                                 "  'sink.partition-commit.delay'='1 m',\n" +
                "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
                ")";
        tEnv.executeSql(hiveSql);

        String insertSql = "insert into fs_table1111 SELECT code, total_emp, " +
                " DATE_FORMAT(r_t, 'yyyy-MM-dd'), DATE_FORMAT(r_t, 'HH') FROM kafkaTable22";
        tEnv.executeSql(insertSql).print();

    }
}
