package com.kouyy.flink.sinks;

import com.google.gson.JsonArray;
import com.kouyy.flink.pojo.AlertMessage;
import com.kouyy.flink.utils.BeanConverter;
import com.kouyy.flink.utils.HttpClient;
import com.kouyy.flink.utils.JdbcUtil;
import com.kouyy.flink.utils.TimeUtil;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * 给AM发送邮件
 * 先提醒，提醒成功插入数据库；再查询是否7天内在唯一ID维度有两个不同唯一ID提醒超过2次就警告，警告成功插入数据库
 * Tuple6<String,String,String,String,String,String>(talk_id,ten_id,land_id,role,storeContent,message)
 */
public class ImMonitorMysqlSinkA extends RichSinkFunction<Tuple6<String, String, String, String, String, String>> {

    private final static Logger logger = LoggerFactory.getLogger(ImMonitorMysqlSinkA.class);
    Connection conn =null;

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn=JdbcUtil.getConnection();
        conn.setAutoCommit(false);
    }

    @Override
    public void invoke(Tuple6<String, String, String, String, String, String> tuple, Context context) throws Exception {
        String alertUrl="http://test-xzremindbase-00.test1.xiaozhu.com?op=createRemindByBiz";
        String punishtUrl="http://test-xzremindbase-00.test1.xiaozhu.com?op=createRemindByBiz";

//        String alertUrl="http://service-remind.xiaozhu.com?op=createRemindByBiz";
        String alert_response="init_response";
        try{
//            alert_response = sendPostRequest(alertUrl, tuple,"YY800");
        }catch (Exception e){
            logger.error("调用接口异常,接口为{},接口返回值为 {} ",alertUrl,alert_response);
        }
         if(alert_response.contains("200")){
             insertDB(tuple, "提醒成功");
         }else{
             insertDB(tuple, "提醒失败");
             logger.error("调用接口异常,接口为{},接口返回值为 {} ",alertUrl,alert_response);
         }
        String last7days_timestamp = TimeUtil.stampToBeijingDate2Second(Long.toString((System.currentTimeMillis()/1000-7*24*60*60)*1000), 8);
        int count = queryDB(tuple, last7days_timestamp);
        if(count>=2){
            //调研惩戒接口
            String punish_response="init_response";
            try{
//                punish_response = sendPostRequest(punishtUrl, tuple,"YY800");
            }catch (Exception e){
                logger.error("调用接口异常,接口为{},接口返回值为 {} ",punishtUrl,punish_response);
            }
            if(punish_response.contains("200")){
                insertDB(tuple, "惩戒成功");
            }else{
                insertDB(tuple, "惩戒失败");
                logger.error("调用接口异常,接口为{},接口返回值为 {} ",punishtUrl,punish_response);
            }
        }
    }

    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
    }


    public boolean insertDB(Tuple6<String, String, String, String, String, String> tuple,String alertType) throws SQLException {
        String sql = "INSERT INTO im_monitor_alert_record ( landlord_id,talk_id,alert_type )VALUES(?,?,?)";
        Boolean result=false;
        try {
            if(conn==null){
                conn=JdbcUtil.getConnection();
                conn.setAutoCommit(false);
            }
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString(1, tuple.f2);
            preparedStatement.setString(2, tuple.f0);
            preparedStatement.setString(3, alertType);
            int i = preparedStatement.executeUpdate();
            if(i==1){
                result=true;
            }
            conn.commit();
        } catch (Exception e) {
            conn.rollback();
            logger.error("插入数据库 {} 失败,插入对象为 {} ", "im_monitor_alert_record", tuple);
        }
        if (!result) {
            logger.error("插入数据库 {} 失败,插入对象为 {} ", "im_monitor_alert_record", tuple);
        }
        return result;
    }


    public int queryDB(Tuple6<String, String, String, String, String, String> tuple,String time) {
        String sql="select count(1) count from(select landlord_id,talk_id,count(1) c from xz_data_monitor.im_monitor_alert_record where alert_type='提醒成功' and landlord_id=? \n" +
                "and create_time >= ? group by landlord_id,talk_id) as tmp group by tmp.landlord_id";
        int count=0;
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString(1, tuple.f2);
            preparedStatement.setString(2, time);
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                count = resultSet.getInt("count");
            }
        }catch (Exception e){
            logger.error("查询数据库 {} 失败", "im_monitor_alert_record");
        }
        return count;
    }


    public String sendPostRequest(String baseUrl,AlertMessage alertMessage,String eventNum) throws ParseException {
        Map<String,Object> map1=new HashMap<String,Object>();
        Map<String,Object> map2=new HashMap<String,Object>();
        Map<String,Object> map3=new HashMap<String,Object>();
        Map<String,Object> map4=new HashMap<String,Object>();
        Map<String,Object> map5=new HashMap<String,Object>();

        JsonArray array1 = new JsonArray();
        array1.add(alertMessage.getAmId());
        JsonArray array2 = new JsonArray();
        array2.add(eventNum);

        map2.put(eventNum,map3);
        map3.put(alertMessage.getAmId().trim(),map4);
        map4.put("callback","");
        map4.put("bizId",Long.parseLong(alertMessage.getAmId().trim()));
        map4.put("bizType",3);
        map4.put("replaceText",map5);
        map5.put("#landlord_id#",alertMessage.getLandlordId().trim());
        map5.put("#landlord_name#",alertMessage.getLandlordName().trim());
        map5.put("#order_id#",alertMessage.getOrderId().trim());
        map5.put("#luId#",alertMessage.getLuId().trim());
        map5.put("#alertType#",alertMessage.getAlertType().trim());
        map5.put("#tenant_id#",alertMessage.getTenantId().trim());

        map1.put("receiverUserId",array1.toString());
        map1.put("remindEventNo",array2.toString());
        map1.put("receiveParams", BeanConverter.mapToJson(map2));

        String response="response_init";
        try{
            response = HttpClient.httpHttpFormData(baseUrl,map1);
//            JSONObject jsonObject = BeanConverter.mapToJson(map1);
//            System.out.println(jsonObject.toString());
        }catch (Exception e){
            e.printStackTrace();
            response=e.getMessage();
        }
        return response;
    }

}
