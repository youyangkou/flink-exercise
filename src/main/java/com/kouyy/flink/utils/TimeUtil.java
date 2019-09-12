package com.kouyy.flink.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

public class TimeUtil {
    //每5分钟获取当前5分钟开始时间的时间戳
    public static Long timeSlot() {
        long now = System.currentTimeMillis()/1000;
        long yu = now % 300;
        return (now - yu);
    }

    //获取指定时间戳对应当天5分钟窗口开始时间的时间戳
    public static Long timeSlot(long timestamp) {
        long now = timestamp/1000;
        long yu = now % 300;
        return (now - yu);
    }

    //获取当天时间，北京时间，格式为yyyyMMdd
    public static String getToday() {
        return stampToBeijingDate(Long.toString(System.currentTimeMillis()), 8);
    }

    //获取当天时间，自定义时区，格式为yyyyMMdd
    public static String getToday(float timeZoneOffset) {
        return stampToBeijingDate(Long.toString(System.currentTimeMillis()), timeZoneOffset);
    }

    //获取当天时间，北京时间,格式为yyyy-MM-dd HH:mm:ss
    public static String getToday2Second() {
        return stampToBeijingDate2Second(Long.toString(System.currentTimeMillis()), 8);
    }

    //获取当天时间，自定义时区,格式为yyyy-MM-dd HH:mm:ss
    public static String getToday2Second(float timeZoneOffset) {
        return stampToBeijingDate2Second(Long.toString(System.currentTimeMillis()), timeZoneOffset);
    }


    /**
     * 将时间戳转换为北京时间，格式为yyyyMMdd
     * timeZoneOffset原为int类型，为班加罗尔调整成float类型
     * timeZoneOffset表示时区，如中国一般使用东八区，因此timeZoneOffset就是8
     * @return
     */
    public static String stampToBeijingDate(String s,float timeZoneOffset){
        if (timeZoneOffset > 13 || timeZoneOffset < -12) {
            timeZoneOffset = 0;
        }
        int newTime=(int)(timeZoneOffset * 60 * 60 * 1000);
        TimeZone timeZone;
        String[] ids = TimeZone.getAvailableIDs(newTime);
        if (ids.length == 0) {
            timeZone = TimeZone.getDefault();
        } else {
            timeZone = new SimpleTimeZone(newTime, ids[0]);
        }
        String res;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(timeZone);
        long lt = new Long(s);
        Date date = new Date(lt);
        res = sdf.format(date);
        return res;
    }

    /**
     * 将时间戳转换为北京时间，格式为yyyy-MM-dd HH:mm:ss
     * timeZoneOffset原为int类型，为班加罗尔调整成float类型
     * timeZoneOffset表示时区，如中国一般使用东八区，因此timeZoneOffset就是8
     * @return
     */
    public static String stampToBeijingDate2Second(String s,float timeZoneOffset){
        if (timeZoneOffset > 13 || timeZoneOffset < -12) {
            timeZoneOffset = 0;
        }
        int newTime=(int)(timeZoneOffset * 60 * 60 * 1000);
        TimeZone timeZone;
        String[] ids = TimeZone.getAvailableIDs(newTime);
        if (ids.length == 0) {
            timeZone = TimeZone.getDefault();
        } else {
            timeZone = new SimpleTimeZone(newTime, ids[0]);
        }
        String res;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(timeZone);
        long lt = new Long(s);
        Date date = new Date(lt);
        res = sdf.format(date);
        return res;
    }


    //字符串转日期，pattern为 "yyyy-MM-dd HH:mm:ss"或者其他格式
    public static Date str2Date(String strDate,String pattern) {
        SimpleDateFormat df=new SimpleDateFormat(pattern);
        Date date=new Date();
        try {
            date=df.parse(strDate);
            System.out.println(date);
        } catch(ParseException px) {
            px.printStackTrace();
        }
        return date;
    }

    /**
     * 将时间转换为时间戳
     */
    public static long dateToStamp(String s) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        return date.getTime();
    }

}
