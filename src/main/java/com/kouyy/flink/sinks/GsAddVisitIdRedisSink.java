package com.kouyy.flink.sinks;

import com.kouyy.flink.utils.RedisUtil;
import com.kouyy.flink.utils.TimeUtil;
import com.kouyy.flink.utils.RedisUtil;
import com.kouyy.flink.utils.TimeUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.JedisCluster;

public class GsAddVisitIdRedisSink extends RichSinkFunction<String> {

    @Override
    public void invoke(String visitId, Context context) {
        String devicesKey="bdp_search_devices_"+ TimeUtil.getToday();

        JedisCluster jedisCluster = null;
        try{
            jedisCluster = RedisUtil.getJedisCluster();
            Boolean res = jedisCluster.exists(devicesKey);
            if(!res){
                jedisCluster.sadd(devicesKey,visitId);
                jedisCluster.expire(devicesKey, 32 * 24 * 60 * 60);
            }else{
                jedisCluster.sadd(devicesKey,visitId);
            }
        }catch(Exception e){
            System.err.println("sink中redis操作异常"+e.getMessage());
        }
    }


}
