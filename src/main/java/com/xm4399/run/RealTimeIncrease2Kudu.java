package com.xm4399.run;


import com.xm4399.util.JDBCUtil;
import org.apache.commons.math3.random.ISAACRandom;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.xm4399.util.KafkaStringSchema;
import com.xm4399.util.KuduSink;

import java.util.Properties;

public class RealTimeIncrease2Kudu {

    public static void main(String[] args) {
        String jobID = args[0];
        String[] conInfoArr = JDBCUtil.getConfInfoArr(jobID);
        String address = conInfoArr[0];
        String username = conInfoArr[1];
        String password = conInfoArr[2];
        String dbName = conInfoArr[3];
        String tableName = conInfoArr[4];
        String isSubTable = conInfoArr[6];
        String topic = conInfoArr[7];
        String kuduTableName = conInfoArr[8];
        realTimeIncrease(address, username, password, dbName, tableName, isSubTable, topic,kuduTableName, jobID);
    }

    public static void realTimeIncrease (String address, String username, String password, String dbName, String tableName,
                                         String isSubTable, String topic, String kuduTableName, String jobID) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  从kafka中读取数据
        // 创建kafka相关的配置
        Properties properties = new Properties();
        String consumerGroupName = tableName + "_" + jobID;
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("bootstrap.servers", "10.0.0.194:9092,10.0.0.195:9092,10.0.0.199:9092");
        properties.setProperty("group.id", consumerGroupName);
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //properties.setProperty("auto.offset.reset", "earliest");
        Properties props = new Properties();

        FlinkKafkaConsumer<ConsumerRecord<String,String>> consumer
                = new FlinkKafkaConsumer<ConsumerRecord<String,String>>(topic, new KafkaStringSchema(), properties);

        consumer.setStartFromTimestamp(System.currentTimeMillis() - 1800000); //从一个小时前开始消费
        DataStreamSink<ConsumerRecord<String,String>> stream = env
                .addSource(consumer)
                .addSink(new KuduSink(address, username, password, dbName, tableName, isSubTable, topic, kuduTableName));

        /*DataStreamSink<ConsumerRecord<String,String>> stream = env
                .addSource(new FlinkKafkaConsumer<ConsumerRecord<String,String>>(isRealtime, new KafkaStringSchema(), properties))
                .addSink(new KuduSink(address, username, password, dbName, tableName, isSubTable, isRealtime, kuduTableName));*/
        // .addSource(new FlinkKafkaConsumer<ConsumerRecord<String,String>>("qianduan_test", new KafkaStringSchema(), properties))
        try {
            env.execute();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

}
