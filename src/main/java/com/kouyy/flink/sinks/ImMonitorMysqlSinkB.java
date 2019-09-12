package com.kouyy.flink.sinks;

import com.kouyy.flink.utils.JdbcUtil;
import com.kouyy.flink.utils.JdbcUtil;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * 给AM发送邮件
 * Tuple6<String,String,String,String,String,String>(talk_id,ten_id,land_id,role,storeContent,message)
 */
public class ImMonitorMysqlSinkB extends RichSinkFunction<Tuple6<String, String, String, String, String, String>> {

    private final static Logger logger = LoggerFactory.getLogger(ImMonitorMysqlSinkB.class);
    Connection conn =null;

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn= JdbcUtil.getConnection();
        conn.setAutoCommit(false);
    }

    @Override
    public void invoke(Tuple6<String, String, String, String, String, String> tuple, Context context) throws Exception {
        String sql="INSERT INTO im_monitor_message_record ( landlord_id,talk_id,im_message)VALUES(?,?,?)";
        int i=0;
        try{
            if(conn==null){
                conn=JdbcUtil.getConnection();
                conn.setAutoCommit(false);
            }
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString(1, tuple.f2);
            preparedStatement.setString(2, tuple.f0);
            preparedStatement.setString(3, tuple.f5);
            i = preparedStatement.executeUpdate();
            conn.commit();
        }catch(Exception e){
            conn.rollback();
            logger.error("插入数据库 {} 失败,message为 {} ","im_monitor_message_record",tuple.f5);
        }
        if(i!=1){
            logger.error("插入数据库 {} 失败,message为 {} ","im_monitor_message_record",tuple.f5);
        }
    }

    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
    }

}
