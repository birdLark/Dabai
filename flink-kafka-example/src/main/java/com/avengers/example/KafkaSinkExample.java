package com.avengers.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaSinkExample {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkExample.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //@TODO checkpoint配置
        //@TODO 设置statebackend
        DataStreamSource<String> text = env.socketTextStream("192.168.1.204", 9000);
        //仅一次语义的kafka的producer
        String topicId = "t1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.1.204:9092");
        //如果时间问题:
        // 1.1设置FlinkKafkaProducer的超时时间
        //1.2设置kafka的最大超时时间server.properties
//        prop.setProperty("transaction.max.timeout.ms","900000");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(topicId, new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        text.addSink(producer);
        env.execute("KafkaSinkExample");
    }
}
