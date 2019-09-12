package com.kouyy.flink.function;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.gson.Gson;
import com.kouyy.flink.utils.RedisUtil;
import com.kouyy.flink.utils.TimeUtil;
import com.kouyy.flink.utils.BeanConverter;
import com.kouyy.flink.utils.StringUtil;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 用户增加唯一标识
 */
public class GsAddVisitIdWindowFunction extends ProcessWindowFunction<Tuple4<String, String,String,Long>, String, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Tuple4<String, String,String,Long>> elements, Collector<String> out) throws Exception {
        List<String> prices=new ArrayList<>();
        List<String>  bedNums=new ArrayList<>();
        List<String> guestNums=new ArrayList<>();
        List<String> houseTypes=new ArrayList<>();
        List<String> leaseTypes=new ArrayList<>();
        List<String> clickEmb=new ArrayList<>();
        String user_id="";

        //Tuple4<>(u,cs1,luid,tm)
        for (Tuple4<String, String,String,Long> tuple: elements) {
            String luidRes =null;
            try{
                luidRes = RedisUtil.getJedisCluster().get("bdp_search_lu_info_"+tuple.f2);
            }catch (Exception e){
                System.err.println("获取redis连接异常"+e.getMessage());
            }
            if(!StringUtil.isEmptyOrWhiteSpace(luidRes)){
                Gson gson = new Gson();
                HashMap<String,String> gsRedisInput = gson.fromJson(luidRes, HashMap.class);
                if(!gsRedisInput.isEmpty()){
                    prices.add(gsRedisInput.get("price"));
                    bedNums.add(gsRedisInput.get("bed_num"));
                    guestNums.add(gsRedisInput.get("guest_num"));
                    houseTypes.add(gsRedisInput.get("house_type"));
                    leaseTypes.add(gsRedisInput.get("lease_type"));
                    clickEmb.add(gsRedisInput.get("click_emb"));
                }
            }
            user_id=tuple.f1;
        }

        Boolean res1 = RedisUtil.getJedisCluster().exists("bdp_search_device_action_"+key);
        if(!res1){
            RedisUtil.getJedisCluster().hset("bdp_search_device_action_"+key,"user_id",user_id);
            RedisUtil.getJedisCluster().expire("bdp_search_device_action_"+key, 32 * 24 * 60 * 60);
        }else{
            RedisUtil.getJedisCluster().hset("bdp_search_device_action_"+key,"user_id",user_id);
        }

          Long aLong = TimeUtil.timeSlot();
//        System.out.println("uesrId:"+key+"----"+ "窗口时间:"+aLong);
        //先查询出redis里面的数据，合并后再插入
        String hget = RedisUtil.getJedisCluster().hget("bdp_search_device_action_" + key, aLong.toString());
        if(hget!=null){
            com.alibaba.fastjson.JSONObject json = JSON.parseObject(hget);

            JSONArray bed_numArr = null;
            JSONArray guest_numArr = null;
            JSONArray house_typeArr = null;
            JSONArray lease_typeArr = null;
            JSONArray priceArr = null;
            JSONArray click_embArr = null;

            if(json!=null){
                 bed_numArr = json.getJSONArray("bed_num");
                 guest_numArr = json.getJSONArray("guest_num");
                 house_typeArr = json.getJSONArray("house_type");
                 lease_typeArr = json.getJSONArray("lease_type");
                 priceArr = json.getJSONArray("price");
                 click_embArr = json.getJSONArray("click_emb");
            }

            if(bed_numArr!=null&&bed_numArr.size()>0){
                for (Object o : bed_numArr) {
                    bedNums.add((String)o);
                }
            }
            if(guest_numArr!=null&&guest_numArr.size()>0){
                for (Object o : guest_numArr) {
                    guestNums.add((String)o);
                }
            }
            if(house_typeArr!=null&&house_typeArr.size()>0){
                for (Object o : house_typeArr) {
                    houseTypes.add((String)o);
                }
            }
            if(lease_typeArr!=null&&lease_typeArr.size()>0){
                for (Object o : lease_typeArr) {
                    leaseTypes.add((String)o);
                }
            }
            if(priceArr!=null&&priceArr.size()>0){
                for (Object o : priceArr) {
                    prices.add((String)o);
                }
            }
            if(click_embArr!=null&&click_embArr.size()>0){
                for (Object o : click_embArr) {
                    clickEmb.add((String)o);
                }
            }
        }

        //转换为map
        Map<String,List<String>> redisMap=new HashMap();
        if(prices!=null&&prices.size()>0){
            redisMap.put("price",prices);
        }
        if(bedNums!=null&&bedNums.size()>0){
            redisMap.put("bed_num",bedNums);
        }
        if(guestNums!=null&&guestNums.size()>0){
            redisMap.put("guest_num",guestNums);
        }
        if(houseTypes!=null&&houseTypes.size()>0){
            redisMap.put("house_type",houseTypes);
        }
        if(leaseTypes!=null&&leaseTypes.size()>0){
            redisMap.put("lease_type",leaseTypes);
        }
        if(clickEmb!=null&&clickEmb.size()>0){
            redisMap.put("click_emb",clickEmb);
        }
        JSONObject jsonObject =null;
        if(redisMap!=null&&!redisMap.isEmpty()){
            jsonObject = BeanConverter.mapToJson(redisMap);
        }
//        System.out.println("用户5分钟特征:"+jsonObject);
        if(jsonObject!=null&&!jsonObject.isEmpty()){
            try{
                Boolean res2 = RedisUtil.getJedisCluster().exists("bdp_search_device_action_"+key);
                if(!res2){
                    RedisUtil.getJedisCluster().hset("bdp_search_device_action_"+key,aLong.toString(),jsonObject.toString());
                    RedisUtil.getJedisCluster().expire("bdp_search_device_action_"+key, 32 * 24 * 60 * 60);
                }else{
                    RedisUtil.getJedisCluster().hset("bdp_search_device_action_"+key,aLong.toString(),jsonObject.toString());
                }
            }catch (Exception e){
                System.err.println("redis操作异常"+e.getMessage());
            }
        }
        out.collect(key);
    }
}
