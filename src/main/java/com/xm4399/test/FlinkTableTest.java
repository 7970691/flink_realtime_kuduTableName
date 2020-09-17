package com.xm4399.test;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;

/**
 * @Auther: czk
 * @Date: 2020/9/11
 * @Description:
 */
public class FlinkTableTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(30001);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(env, bsSettings);
        String str = "CREATE TABLE order_info(\n" +
                "    id BIGINT,\n" +
                "    user_id BIGINT,\n" +
                "    create_time TIMESTAMP(0),\n" +
                "    operate_time TIMESTAMP(0),\n" +
                "    province_id INT,\n" +
                "    order_status STRING,\n" +
                "    total_amount DECIMAL(10, 5)\n" +
                "  ) WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'a5515458',\n" +
                "    'database-name' = 'chenzhikun',\n" +
                "    'table-name' = 'order_info'\n" +
                ")";
        stenv.executeSql(str);
        //stenv.executeSql(str).print();
       // TableResult a ;

        String query = "select *  from order_info";
        stenv.executeSql(query).print();

       /* stenv.executeSql("CREATE TABLE kafka_test_format (\n" +
                "      day_str STRING \n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'flink_cdc_test',\n" +
                "    'properties.bootstrap.servers'     = 'localhost:9092',\n" +
                "    'format'     = 'changelog-json',\n" +
                "    'scan.startup.mode'     = 'earliest-offset'\n" +
                ")");


        String insert = "INSERT INTO kafka_test_format\n" +
                "SELECT create_time as day_str \n" +
                "FROM default_database.order_info " ;
        stenv.executeSql(insert).print();*/

        /*String query = "select *  from kafka_test_format";
        stenv.executeSql(query).print();*/
    }
}
