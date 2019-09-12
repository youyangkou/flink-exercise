package com.kouyy.flink.pro;

import com.google.gson.Gson;
import com.kouyy.flink.pojo.BookOrder;
import com.kouyy.flink.pojo.IMTenantAndLuId;
import com.kouyy.flink.pojo.IMmessage;
import com.kouyy.flink.pojo.OrderMessage;
import com.kouyy.flink.utils.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 定向召回发券项目
 */
public class RecallHandler {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new MemoryStateBackend());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(60000);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //设置job失败重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                Time.of(5, TimeUnit.MINUTES),
                Time.of(10, TimeUnit.SECONDS)
        ));

        //IM
        RMQConnectionConfig imConnectionConfig = new RMQConnectionConfig.Builder()
                .setHost(FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("im.host"))
                .setPort(Integer.parseInt(FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("im.port")))
                .setUserName(FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("im.userName"))
                .setPassword(FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("im.password"))
                .setVirtualHost(FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("im.virtualHost"))
                .build();

        //Order
        RMQConnectionConfig orderCnnectionConfig = new RMQConnectionConfig.Builder()
                .setHost(FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("order.host"))
                .setPort(Integer.parseInt(FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("order.port")))
                .setUserName(FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("order.userName"))
                .setPassword(FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("order.password"))
                .setVirtualHost(FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("order.virtualHost"))
                .build();

        //orderDataStream
        final DataStream<String> orderDataStream =
                env.addSource(new RMQSource<String>(
                        orderCnnectionConfig,
                        FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("order.queueName"),
                        false,
                        new SimpleStringSchema()));

        //IMdataStream
        final DataStream<String> imDataStream =
                env.addSource(new RMQSource<String>(
                        imConnectionConfig,
                        FlinkPropertyUtil.getMapFromProperties("rabbitmq.properties").get("im.queueName"),
                        false,
                        new SimpleStringSchema()));

        //IM 数据转换为IMTalkMsgIdAndTenantAndLuId
        DataStream<IMTenantAndLuId> iMmessageFilterDataStream  = imDataStream.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        try{
                            if(!StringUtil.isEmptyOrWhiteSpace(value)){
                                Gson gson = new Gson();
                                IMmessage iMmessage = gson.fromJson(value, IMmessage.class);
                                if(iMmessage!=null&&!StringUtil.isEmptyOrWhiteSpace(iMmessage.getImTalkMsgId().toString())){
                                    if(!"97992314101".equals(iMmessage.getFromUserId())&&!"97992314101".equals(iMmessage.getToUserId())){
                                        return true;
                                    }
                                }
                            }
                        }catch (Exception e){
                        }
                        return false;
                    }
                }).map(
                new MapFunction<String, IMTenantAndLuId>() {
                    @Override
                    public IMTenantAndLuId map(String value) throws Exception {
                        if(!StringUtil.isEmptyOrWhiteSpace(value)){
                            Gson gson = new Gson();
                            IMmessage iMmessage = gson.fromJson(value, IMmessage.class);
                            if(iMmessage!=null&&!StringUtil.isEmptyOrWhiteSpace(iMmessage.getImTalkMsgId().toString())){
                                IMTenantAndLuId imTenantAndLuId=null;
                                //排除IM机器人
                                if(!"97992314101".equals(iMmessage.getFromUserId())&&!"97992314101".equals(iMmessage.getToUserId())){
                                    if(("tenant").equals(iMmessage.getFromUserIdUserRole())){
                                        imTenantAndLuId=new IMTenantAndLuId(iMmessage.getFromUserId()+"_"+iMmessage.getLuId(),iMmessage.getFromUserId(),iMmessage.getStoreContent(),"from");
                                        //插入redis一条记录；key:Tenantid_AndLuId  value:访问时间戳
                                        Long resz = RedisUtil.setListLeftKV(imTenantAndLuId.getTenantId_luid(), 600, Long.toString(System.currentTimeMillis()));
                                        int retrie=0;
                                        while(resz==null&&retrie<=3){
                                            retrie++;
                                            resz=RedisUtil.setListLeftKV(imTenantAndLuId.getTenantId_luid(), 600, Long.toString(System.currentTimeMillis()));
                                        }
                                    }
                                    if(("landlord").equals(iMmessage.getFromUserIdUserRole())){
                                        imTenantAndLuId=new IMTenantAndLuId(iMmessage.getToUserId()+"_"+iMmessage.getLuId(),iMmessage.getToUserId(),iMmessage.getStoreContent(),"to");
                                    }
                                    return imTenantAndLuId;
                                }
                            }
                        }
                        return new IMTenantAndLuId();
                    }
                });


        //processFunction
        DataStream<Tuple2<String,String>> windowOut = iMmessageFilterDataStream.keyBy(t -> t.getTenantId_luid())
                .timeWindow(org.apache.flink.streaming.api.windowing.time.Time.minutes(5), org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
                .process(new ProcessWindowFunction<IMTenantAndLuId, Tuple2<String,String>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<IMTenantAndLuId> elements, Collector<Tuple2<String,String>> out) throws Exception {
                        int count=0;
                        for (IMTenantAndLuId element : elements) {
                            if(element!=null&&!StringUtil.isEmptyOrWhiteSpace(element.getStoreContent())){
                                count++;
                            }
                        }
                        //count=0表示5分钟内没有一个人说话
                        if(count>1){
                            key="";
                        }
                        //count==1查询redis判断最近的一次IM谈话话间隔
                        if(!StringUtil.isEmptyOrWhiteSpace(key)){
                            String res = RedisUtil.getListIndexKV(key, 0);
                            if(!StringUtil.isEmptyOrWhiteSpace(res)){
                                long latestTime = Long.parseLong(res);
                                if((System.currentTimeMillis() - latestTime) / (1000)>290 && (System.currentTimeMillis() - latestTime) / (1000)<305){
                                    String[] s = key.split("_");
                                    out.collect(new Tuple2<String,String>(s[0],s[1]));
                                }
                            }
                        }
                    }
                });

        windowOut.addSink(new RichSinkFunction<Tuple2<String,String>>() {
            private static final long serialVersionUID = -4443175430371919407L;
            PreparedStatement ps;
            private Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                connection = JdbcUtil.getConnection();
//                String sql = "select * from xz_marketing.bookorderstate where operator_id=? and luid=? order by operator_time desc limit 1";
                String sql = "select * from xz_marketing.bookorderstate where operator_id=? order by operator_time desc limit 1";
                ps = this.connection.prepareStatement(sql);
            }

            @Override
            public void close() throws Exception {
                super.close();
                //关闭连接和释放资源
                if (connection != null) {
                    connection.close();
                }
                if (ps != null) {
                    ps.close();
                }
            }

            //判断房客是否下单
            public  boolean queryIsCreateOrder(ResultSet resultSet ){
                boolean flag=false;
                try {
                    String bookorder_id ="";
                    String currentstate ="";
                    String laststate ="";
                    String operator_time ="";
                    String operator_last_day="";
                    while(resultSet.next()){
                        bookorder_id = resultSet.getString("bookorder_id");
                        currentstate = resultSet.getString("currentstate");
                        laststate = resultSet.getString("laststate");
                        operator_time = resultSet.getString("operator_time");
                    }
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                    String today = df.format(new Date());
                    if(!StringUtil.isEmptyOrWhiteSpace(operator_time)){
                        String[] s = operator_time.split(" ");
                        if(today.equals(s[0])){
                            operator_last_day=today;
                        }
                    }
//                    if(!StringUtil.isEmptyOrWhiteSpace(bookorder_id)&&!StringUtil.isEmptyOrWhiteSpace(currentstate)&&operator_last_day.equals(today)){
//                        flag=true;
//                    }
                    if(!StringUtil.isEmptyOrWhiteSpace(bookorder_id)&&operator_last_day.equals(today)){
                        flag=true;
                    }
                }catch (Exception e){
                    System.err.println("操作数据库异常"+e.getMessage());
                }
                return flag;
            }

            @Override
            public void invoke(Tuple2<String,String> value, Context context) throws Exception {
                if(value!=null) {
                    //查询这个房客和这个房源今天有没有被推荐过
                    String pushKey="im_push_users_"+ TimeUtil.getToday();
                    if(!RedisUtil.isExistsPushKeyvalue(pushKey,value.f0 + "_" + value.f1)){
                        ps.setString(1, value.f0);
//                        ps.setString(2, value.f1);
                        if (!queryIsCreateOrder(ps.executeQuery())) {
                            String res = RecallUtil.sendGet(value.f0, value.f1, "XZH400");
                            if(res!=null){
//                                System.out.println("推荐房源接口请求成功");
                                if(RedisUtil.delKV(value.f0 + "_" + value.f1)!=null){
                                    RedisUtil.setPushKeyvalue(pushKey,value.f0 + "_" + value.f1);
                                }
                            }
                        }
                    }
                }
            }
        });



        //ordermessageMapStream
        DataStream<OrderMessage> ordermessageMapStream = orderDataStream.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if(!StringUtil.isEmptyOrWhiteSpace(value)){
                            Gson gson = new Gson();
                            OrderMessage orderMessage = gson.fromJson(value, OrderMessage.class);
                            if(orderMessage!=null){
                                return true;
                            }
                        }
                        return false;
                    }
                }).map(
                new MapFunction<String, OrderMessage>() {
                    @Override
                    public OrderMessage map(String value) throws Exception {
                        Gson gson = new Gson();
                        OrderMessage orderMessage = gson.fromJson(value, OrderMessage.class);
                        if(orderMessage.getContent()!=null){
                            BookOrder bookOrder = orderMessage.getContent().getBookOrder();
                            try{
                                if(orderMessage.getOrderState()!=null&&orderMessage.getOrderState().equals("confirmTimeOut")&&bookOrder.getLastState()!=null&&bookOrder.getLastState().equals("submitted")){
                                    String res = RecallUtil.sendGet(bookOrder.getSubmitterId(), bookOrder.getLuId(), "XZH500");
                                }else if(orderMessage.getOrderState()!=null&&orderMessage.getOrderState().equals("reject")&&bookOrder.getOperatorId()!=null&&bookOrder.getLandlordId()!=null&&bookOrder.getOperatorId().equals(bookOrder.getLandlordId())){
                                    String res = RecallUtil.sendGet(bookOrder.getSubmitterId(), bookOrder.getLuId(), "XZH600");
                                }else if(orderMessage.getOrderState()!=null&&orderMessage.getOrderState().equals("cancel")&&!bookOrder.getOperatorId().equals(bookOrder.getLandlordId())){
                                    String res = RecallUtil.sendGet(bookOrder.getSubmitterId(), bookOrder.getLuId(), "XZH700");
                                }
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                        return orderMessage;
                    }
                });

        env.execute("RecallHandler");


    }
}
