package com.avengers.example;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 测试数据:
 *
 *book bigdata kafka,es,redis
 *book javaweb mybatis,hibernate
 *book java    array,int
 *
 */
//单并行度的source
public class RedisSource implements SourceFunction<Map<String,String>> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSource.class);
    private Jedis jedis = null;
    private boolean isRunning =true ;
    private final long sleepTime = 60000;

    public void run(SourceContext<Map<String, String>> sourceContext) {
        this.jedis= new Jedis("192.168.1.204",6379);
        Map<String,String> book = new HashMap<String, String>();
        while(isRunning){
            try{
                book.clear();
                Map<String, String> books = jedis.hgetAll("book");
                for (Map.Entry<String, String> entrySet : books.entrySet()){
                    String type = entrySet.getKey();
                    String[] splits = entrySet.getValue().split(",");
                    for (String split: splits) {
                        book.put(split,type);
                    }
                }
                if(book.size()>0){
                    sourceContext.collect(book);
                }else {
                    LOG.info("从redis中获取的数据为空");
                }
                Thread.sleep(sleepTime);
            }catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
    }

    public void cancel() {
        isRunning = false;
        if(jedis != null) {
            jedis.close();
        }
    }
}
