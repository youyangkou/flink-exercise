package com.kouyy.flink.sinks;

import com.kouyy.flink.pojo.OrderMessage;
import com.kouyy.flink.utils.JdbcUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class OrderStateMysqlSink extends RichSinkFunction<OrderMessage> {

    @Override
    public void invoke(OrderMessage orderMessage, Context context) throws Exception {

        if (orderMessage.getContent()!=null&&orderMessage.getContent().getBookOrder()!=null) {
            String currentState = orderMessage.getContent().getBookOrder().getCurrentState();
            String lastState = orderMessage.getContent().getBookOrder().getLastState();
            String operatorId = orderMessage.getContent().getBookOrder().getOperatorId();
            String luId = orderMessage.getContent().getBookOrder().getLuId();
            String orderId = orderMessage.getContent().getBookOrder().getOrderId();
            String createTime = orderMessage.getContent().getBookOrder().getCreateTime();

            String baseSQL="INSERT INTO bookorderstate (bookorder_id,luid,currentstate,laststate,operator_id, operator_time)VALUES('";
            String sql=baseSQL+orderId+"','"+luId+"','"+currentState+"','"+lastState+"','"+operatorId+"','"+createTime+"')";
            Boolean result=false;
            try {
                result = JdbcUtil.insertSQL(sql);
                if(result){
                    System.out.println("插入数据库成功");
                }
            }catch (Exception e){
                System.err.println("插入数据库表异常"+ orderMessage.to_String()+e.getMessage());
            }
        }
    }
}
