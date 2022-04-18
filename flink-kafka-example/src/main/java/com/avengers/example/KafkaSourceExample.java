package com.avengers.example;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class KafkaSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //@TODO checkpoint配置
        //@TODO 设置statebackend
        String topic = "t1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.1.204:9092");
        prop.setProperty("group.id","flinkConsumer");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
        //设置消费策略
        consumer.setStartFromGroupOffsets();
        DataStreamSource<String> text = env.addSource(consumer);
        //将并行度设置为1
        text.print().setParallelism(1);
        env.execute("KafkaSourceExample");
    }
}
