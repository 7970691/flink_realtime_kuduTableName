package com.xm4399.realtime;


import com.xm4399.util.KafkaStringSchema;
import com.xm4399.util.KuduSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class RealTimeIncrease2Kudu {

    public static void main(String[] args) {

        realTimeIncrease();
    }

    public static void realTimeIncrease () {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置无重试模式,job出现意外时直接失败
        env.setRestartStrategy(RestartStrategies.noRestart());
        // 设置check point
        /*env.setStateBackend(new RocksDBStateBackend("hdfs:///checkpoints-data/");
        env.enableCheckpointing(60 * 1000);*/
        Properties properties = new Properties();

        properties.setProperty("enable.auto.commit", "true");

        //properties.setProperty("bootstrap.servers", "10.0.0.194:9092,10.0.0.195:9092,10.0.0.199:9092");
        //properties.setProperty("group.id", consumerGroupName);
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer<ConsumerRecord<String,String>> consumer
                = new FlinkKafkaConsumer<ConsumerRecord<String,String>>("flink_realtime_test", new KafkaStringSchema(), properties);
        //从一个小时前开始消费,避免全量拉取过程中更新日志的丢失
        consumer.setStartFromTimestamp(System.currentTimeMillis() - 120 *60 * 1000);

       /* DataStreamSink<ConsumerRecord<String,String>> stream = env
                .addSource(consumer)
                .addSink(new KuduSink());*/

        try {
            String jobName = "flink_realtime_job_" ;
            //flink任务运行中,更新mysql状态
            //new JDBCUtil().updateJobState(jobID, "2_RealTime");
            env.execute(jobName);
        } catch (Exception exception) {
            // flink任务出现异常,更新mysql状态
            //new JDBCUtil().updateJobState(jobID, "-1_RealTime");
            //new JDBCUtil().insertErroeInfo(jobID, "RealTime", "" );
            exception.printStackTrace();
        }
    }

}
