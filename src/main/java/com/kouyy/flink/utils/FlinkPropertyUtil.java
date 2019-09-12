package com.kouyy.flink.utils;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author kouyouyang
 * @date 2019-09-03 12:02
 */
public class FlinkPropertyUtil implements Serializable {

    private final static Logger logger = LoggerFactory.getLogger(FlinkPropertyUtil.class);

    public static Map<String,String> getMapFromProperties(String  propertiesFileName){
        Map<String,String> map=new HashMap<String,String>();
        Properties props = getPropertiesFile(propertiesFileName);
        try{
            Set<Map.Entry<Object, Object>> entries = props.entrySet();

            for (Map.Entry<Object, Object> entry : entries) {
                String key = (String)entry.getKey();
                String value = (String)entry.getValue();
                map.put(key,value);
            }
        }catch (Exception e) {
            logger.info("获取"+propertiesFileName+"对应的缓存文件异常",e);
        }
        return map;
    }

    public static Map<String,String> getMapFromDB(String sql,String key,String value){
        Map<String,String> map=new HashMap<String,String>();
        String mapKey=key;
        String mapValue=value;
        try {
            ResultSet resultSet = JdbcUtil.querySQL(sql);
            while(resultSet.next()){
                String resultKey = resultSet.getString(mapKey);
                String resultValue = resultSet.getString(mapValue);
                map.put(resultKey,resultValue);
            }
        }catch (Exception e){
            logger.info("操作数据库异常",e);
        }finally {
            try {
                JdbcUtil.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    public static Properties getPropertiesFile(String propertiesFileName) {
        Properties props = new Properties();
        try{
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            InputStream in = classloader.getResourceAsStream(propertiesFileName);
            props.load(new InputStreamReader(new BufferedInputStream(in),"utf-8"));
        }catch (Exception e) {
            logger.info("加载"+propertiesFileName+"配置文件异常",e);
        }
        return props;
    }

    public static Cache<String, String> getMapFromDB(String sql,String key,String value,long size,long duration){
        Cache<String, String> map=null;
        String mapKey=key;
        String mapValue=value;
        try {
            map = CacheBuilder.newBuilder()
                    .maximumSize(size)
                    .expireAfterAccess(duration, TimeUnit.SECONDS)
                    //移除监听器,缓存项被移除时会触发
                    .build();

            ResultSet resultSet = JdbcUtil.querySQL(sql);
            while(resultSet.next()){
                String resultKey = resultSet.getString(mapKey);
                String resultValue = resultSet.getString(mapValue);
                map.put(resultKey,resultValue);
            }
        }catch (Exception e){
            logger.info("操作数据库异常",e);
        }finally {
            try {
                JdbcUtil.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return map;
    }

}
