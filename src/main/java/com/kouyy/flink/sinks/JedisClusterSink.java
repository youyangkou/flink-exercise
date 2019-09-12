package com.kouyy.flink.sinks;

import com.kouyy.flink.utils.RedisUtil;
import com.kouyy.flink.utils.TimeUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.JedisCluster;

/**
 * JedisCluster
 */
public class JedisClusterSink extends RichSinkFunction<String> {

    JedisCluster jedisCluster = null;

    public void open(Configuration parameters) throws Exception {
        jedisCluster = RedisUtil.getJedisCluster();
    }

    @Override
    public void invoke(String userId, Context context) {
        String usersKey="bdp_search_users_"+ TimeUtil.getToday();

        try{
            Boolean res = jedisCluster.exists(usersKey);
            if(!res){
                jedisCluster.sadd(usersKey,userId);
                jedisCluster.expire(usersKey, 2 * 24 * 60 * 60);
            }else{
                jedisCluster.sadd(usersKey,userId);
            }
        }catch(Exception e){
            System.err.println("sink中redis操作异常"+e.getMessage());
        }
    }


    public void close() {
        if (jedisCluster != null) {
            jedisCluster.close();
        }
    }


}
