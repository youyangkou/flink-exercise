package com.kouyy.flink.sinks;


import com.kouyy.flink.utils.TimeUtil;
import com.kouyy.flink.utils.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * Jedis
 */
public class JedisSink extends RichSinkFunction<Tuple6<String, String, String, String, String, String>> {

    Jedis jedis = null;

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedis = RedisUtil.getWriteJedis();
    }

    @Override
    public void invoke(Tuple6<String, String, String, String, String, String> value, Context context) {
        String key="im_monitor_"+ TimeUtil.getToday();

        try{
            if(jedis==null){
                jedis = RedisUtil.getWriteJedis();
            }
            Boolean res = jedis.exists(key);
            if(!res){
                jedis.sadd(key,value.f4);
                jedis.expire(key, 5 * 60);
            }else{
                jedis.sadd(key,value.f4);
            }
        }catch(Exception e){
            System.err.println("sink中redis操作异常"+e.getMessage());
        }
    }


    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }


}
