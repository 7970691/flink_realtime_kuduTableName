package com.xm4399.run;


import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.xm4399.util.KafkaStringSchema;
import com.xm4399.util.SubTableKuduSink;

import java.util.Properties;

public class RealTimeIncrease2Kudu {

    public static void main(String[] args) {
        realTimeIncrease(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
    }

    public static void realTimeIncrease (String address, String username, String password, String dbName,
                                   String tableName, String isSubTable, String isRealtime, String kuduTableName) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  从kafka中读取数据
        // 创建kafka相关的配置
        Properties properties = new Properties();
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("bootstrap.servers", "10.0.0.194:9092,10.0.0.195:9092,10.0.0.199:9092");
        properties.setProperty("group.id", "aaaaaad");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //properties.setProperty("auto.offset.reset", "earliest");
        Properties props = new Properties();

        //ConfArgsPojo confArgsPojo = new ConfArgsPojo(address, username, password, dbName, tableName, isSubTable, isRealtime);

        DataStreamSink<ConsumerRecord<String,String>> stream = env
                .addSource(new FlinkKafkaConsumer<ConsumerRecord<String,String>>(isRealtime, new KafkaStringSchema(), properties))
                .addSink(new SubTableKuduSink(address, username, password, dbName, tableName, isSubTable, isRealtime, kuduTableName));
        // .addSource(new FlinkKafkaConsumer<ConsumerRecord<String,String>>("qianduan_test", new KafkaStringSchema(), properties))
        try {
            env.execute();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

}
