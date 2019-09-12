package com.kouyy.flink.watermarks;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kouyy.flink.utils.StringUtil;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author kouyouyang
 * @date 2019-08-13 18:45
 */
public class PersonalizedSearchWatermark implements AssignerWithPunctuatedWatermarks<String>{

    public Watermark getCurrentWatermark(String element, long extractedTimestamp) {
        if (element != null && element.contains("tm")) {
            String tm ="";
            try{
                JSONObject jsonObject = JSON.parseObject(element);
                tm=jsonObject.getString("tm");
            }catch (Exception e){
                System.err.println("异常::"+e.getMessage()+"------埋点信息::"+element);
            }
            if(!StringUtil.isEmptyOrWhiteSpace(tm)){
                return new Watermark(Long.parseLong(tm));
            }
        }
        return null;
    }


    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        if (lastElement != null && lastElement.contains("tm")) {
            String tm ="";
            try{
                JSONObject jsonObject = JSON.parseObject(lastElement);
                tm=jsonObject.getString("tm");
            }catch (Exception e){
                System.err.println("异常::"+e.getMessage()+"------埋点信息::"+lastElement);
            }
            if(!StringUtil.isEmptyOrWhiteSpace(tm)){
                return new Watermark(Long.parseLong(tm));
            }
        }
        return null;
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        if (element != null && element.contains("tm")) {
            String tm ="";
            try{
                JSONObject jsonObject = JSON.parseObject(element);
                tm=jsonObject.getString("tm");
            }catch (Exception e){
                System.err.println("异常::"+e.getMessage()+"------埋点信息::"+element);
            }
            if(!StringUtil.isEmptyOrWhiteSpace(tm)){
                return Long.parseLong(tm);
            }
        }
        return 0L;
    }
}
