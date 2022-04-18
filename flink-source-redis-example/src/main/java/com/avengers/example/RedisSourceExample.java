package com.avengers.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

public class RedisSourceExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Map<String, String>> source = env.addSource(new RedisSource());

        //循环遍历Map,进行打印
        source.map(new MapFunction<Map<String, String>, Object>() {
            public Object map(Map<String, String> bookMap) throws Exception {
                for (Map.Entry<String, String> entrySet : bookMap.entrySet()){
                    String key = entrySet.getKey();
                    String value = entrySet.getValue();
                    System.out.println("---"+key+":"+value);
                }
                return null;
            }
        });

        env.execute("RedisSourceExample");
    }
}
