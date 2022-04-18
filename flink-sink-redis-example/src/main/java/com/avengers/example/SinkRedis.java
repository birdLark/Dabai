package com.avengers.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("your host", 9000, "\n");
        DataStream<Tuple2<String, String>> mapStream = socketTextStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String word) throws Exception {
                return new Tuple2<>("1_word", word);
            }
        });
        FlinkJedisPoolConfig config =  new FlinkJedisPoolConfig.Builder().setHost("your host").setPort(6379).build();
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<Tuple2<String, String>>(config,new MyRedisMapper());

        mapStream.addSink(redisSink);
        env.execute("redis_sink");
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {

        //List集合中存放数据
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        //从接收的数据中获取key
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        //从接收的数据中获取value
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
