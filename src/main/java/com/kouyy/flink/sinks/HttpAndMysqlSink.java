package com.kouyy.flink.sinks;

import com.google.gson.JsonArray;
import com.kouyy.flink.pojo.AlertMessage;
import com.kouyy.flink.utils.BeanConverter;
import com.kouyy.flink.utils.HttpClient;
import com.kouyy.flink.utils.JdbcUtil;
import com.kouyy.flink.utils.StringUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * 给AM发送邮件
 */
public class HttpAndMysqlSink extends RichSinkFunction<AlertMessage> {

    @Override
    public void invoke(AlertMessage alertMessage, Context context) throws Exception {
        if(alertMessage.getAlertType()!=null){
            query(alertMessage);
        }
        if(!StringUtil.isEmptyOrWhiteSpace(alertMessage.getAmId())&&!StringUtil.isEmptyOrWhiteSpace(alertMessage.getAlertType())){
            //下线后不发邮件，入库
//            String baseUrl="http://test-xzremindbase-00.test1.xiaozhu.com?op=createRemindByBiz";
//            String baseUrl="http://service-remind.xiaozhu.com?op=createRemindByBiz";
//            String response="init_response";
//            try{
//                response = sendPostRequest(baseUrl, alertMessage,"YY800");
////                System.out.println(response);
//            }catch (Exception e){
//                System.err.println("调用邮件提醒接口异常"+e.getMessage());
//            }
//            if(response.contains("200")){
//                insertAlertRecord(alertMessage,"发送邮件成功");
//                System.out.println("发送邮件成功");
//            }else{
//                insertAlertRecord(alertMessage,"发送邮件失败");
//                System.out.println("发送邮件失败+response为:"+response+"-----alertMessage为"+alertMessage);
//            }
            insertAlertRecord(alertMessage,"不发邮件");
        }else{
            insertAlertRecord(alertMessage,"未查询到房东与AM对应关系");
        }
    }


    public boolean insertAlertRecord(AlertMessage alertMessage,String alertStatus) {
        String baseSQL="INSERT INTO alert_record ( landlord_id,tenant_id,luid,order_id,am_id, alert_type,alert_status)VALUES('";
        String sql=baseSQL+alertMessage.getLandlordId()+"','"+alertMessage.getTenantId()+"','"+alertMessage.getLuId()+"','"+alertMessage.getOrderId()+"','"+alertMessage.getAmId()+"','"
                +alertMessage.getAlertType()+"','"+alertStatus+"')";
        Boolean result=false;
        try {
            result = JdbcUtil.insertSQL(sql);
//            if(result){
//                System.out.println("插入数据库成功");
//            }
        }catch (Exception e){
            System.err.println("插入数据库表异常"+e.getMessage());
        }
        return result;
    }


    public AlertMessage query(AlertMessage alertMessage) {
        //通过landlordId查询mysql获取am_id和landlord_name
        String sql="select * from xz_supply_monitor.landlord_am_relation where landlord_id="+alertMessage.getLandlordId().trim()+" and luid="+alertMessage.getLuId().trim();
        try {
            ResultSet resultSet = JdbcUtil.querySQL(sql);
            while(resultSet.next()){
//                System.out.println("查询到房东房管关系了");
                String landlordName = resultSet.getString("landlord_name");
                String amId = resultSet.getString("am_id");
                alertMessage.setLandlordName(landlordName);
                if(amId!=null){
                    alertMessage.setAmId(amId);
                }
            }
        }catch (Exception e){
            System.err.println("操作数据库异常"+e.getMessage());
        }finally {
            try {
                JdbcUtil.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return alertMessage;
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
//        map4.put("bizId",Long.parseLong(alertMessage.getAmId()));
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
