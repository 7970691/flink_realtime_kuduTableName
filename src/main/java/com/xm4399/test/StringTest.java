package com.xm4399.test;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Auther: czk
 * @Date: 2020/9/18
 * @Description:
 */
public class StringTest {
    public static void main(String[] args) throws Exception {

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                //.databaseList("chenzhikun") // monitor all tables under inventory database
                .tableList("chenzhikun.kafka_format_test3")
                .username("root")
                .password("a5515458")
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StringDebeziumDeserializationSchema stringDebeziumDeserializationSchema = new StringDebeziumDeserializationSchema();
        env
                .addSource(sourceFunction)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
        env.execute();
    }
}
