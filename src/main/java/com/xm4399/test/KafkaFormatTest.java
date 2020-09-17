package com.xm4399.test;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.source.ScanTableSource;

/**
 * @Auther: czk
 * @Date: 2020/9/14
 * @Description:
 */
public class KafkaFormatTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30001);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(env, bsSettings);
        stenv.executeSql("CREATE TABLE order_info (\n" +
                "      id INT,\n" +
                "       appname STRING, \n" +
                "       kind_id_name STRING, \n" +
                "       appname STRING \n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "     'hostname' = '10.0.0.211',\n" +
                "     'port' = '3307',\n" +
                "     'username' = 'gprp',\n" +
                "      'password' = 'gprp@@4399',\n" +
                "        'database-name' = '4399_mobi_services',\n" +
                "      'table-name' = 'tuijian_gameboxGame'\n" +
                ")");

        stenv.executeSql("CREATE TABLE kafka_test_format (\n" +
                "      id INT,\n" +
                "       appname STRING,\n" +
                "       kind_id_name STRING, \n" +
                "       appname STRING \n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'flink_realtime_test',\n" +
                "    'properties.bootstrap.servers'     = '10.0.0.194:9092,10.0.0.195:9092,10.0.0.199:9092',\n" +
                "    'format'     = 'json',\n" +
                "    'scan.startup.mode'     = 'latest-offset'\n" +
                ")");


        /*String insert = "INSERT INTO kafka_test_format\n" +
                "SELECT DATE_FORMAT(create_time, 'yyyy-MM-dd') as day_str, SUM(total_amount) as gmv\n" +
                "FROM default_database.order_info \n" +
                "WHERE order_status = '2' -- 订单已支付\n" +
                "GROUP BY DATE_FORMAT(create_time, 'yyyy-MM-dd') ";*/
        String insert = "INSERT INTO kafka_test_format\n" +
                "SELECT *" +
                "FROM  order_info " ;
        stenv.executeSql(insert).print();

        /*String query = "select *  from kafka_test_format";
        stenv.executeSql(query).print();*/
    }
}
