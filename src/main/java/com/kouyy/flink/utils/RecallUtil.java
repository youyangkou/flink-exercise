package com.kouyy.flink.utils;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class RecallUtil {

    //推荐房源Post
    public static String sendPost(String tenantId, String luid, String eventNum) throws ParseException {
        String baseUrl="http://admin.bi.xiaozhu.com/api/per_push";
        Map<String,String> map=new HashMap<>();
        map.put("tenantId",tenantId);
        map.put("luid",tenantId);
        map.put("tenantId",tenantId);
        String response="";
        try{
            response = HttpClient.sendHttpPost(baseUrl,map);
        }catch (Exception e){
            e.printStackTrace();
            response=e.getMessage();
        }
        return response;
    }

    //推荐房源Get
    public static String sendGet(String tenantId, String luid, String eventNum) throws ParseException {
        //线上URL
        String baseUrl="http://da.piggy.xiaozhu.com/api/per_push";
        //测试URL
//        String baseUrl="http://admin.bi.xiaozhu.com/api/per_push";
        Map<String,String> map=new HashMap<>();
        map.put("tenantId",tenantId);
        map.put("luid",luid);
        map.put("eventNum",eventNum);
        String response="";
        try{
            response = HttpClient.sendHttpGet(baseUrl,map);
        }catch (Exception e){
            e.printStackTrace();
            response=e.getMessage();
        }
        return response;
    }

}
