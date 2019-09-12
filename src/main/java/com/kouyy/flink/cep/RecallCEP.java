package com.kouyy.flink.cep;

import com.google.gson.Gson;
import com.kouyy.flink.pojo.IMTenantAndLuId;
import com.kouyy.flink.pojo.IMmessage;
import com.kouyy.flink.utils.JdbcUtil;
import com.kouyy.flink.pojo.IMTenantAndLuId;
import com.kouyy.flink.pojo.IMmessage;
import com.kouyy.flink.utils.JdbcUtil;
import com.kouyy.flink.utils.StringUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 利用Flink CEP实现房源推荐
 */
public class RecallCEP {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(30000);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //设置job失败重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));

        //IM
        RMQConnectionConfig imConnectionConfig = new RMQConnectionConfig.Builder()
                .setHost("10.3.1.13")
                .setPort(5672)
                .setUserName("service-recall")
                .setPassword("7e5506a5e37e869d6324")
                .setVirtualHost("service-recall")
                .build();

//        //Order
//        RMQConnectionConfig orderCnnectionConfig = new RMQConnectionConfig.Builder()
//                .setHost("10.3.1.13")
//                .setPort(5672)
//                .setUserName("trading_system_ro")
//                .setPassword("ff7888f4cedd55337ab3")
//                .setVirtualHost("trading_system")
//                .build();

        //IMdataStream
        final DataStream<String> imDataStream =
                env.addSource(new RMQSource<String>(
                        imConnectionConfig,
                        "qu.bigdata.recall.imchat.im",
                        false,
                        new SimpleStringSchema()));

//        //orderDataStream
//        final DataStream<String> orderDataStream =
//                env.addSource(new RMQSource<String>(
//                        orderCnnectionConfig,
//                        "q.bigdata.recall.orderstate",
//                        false,
//                        new SimpleStringSchema()));

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
                                    if("105229373001".equals(iMmessage.getFromUserId())){
                                        System.out.println(iMmessage);
                                    }
                                    return true;
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
                                if(("tenant").equals(iMmessage.getFromUserIdUserRole())){
                                    return new IMTenantAndLuId(iMmessage.getFromUserId()+"_"+iMmessage.getLuId(),iMmessage.getFromUserId(),iMmessage.getStoreContent(),"from");
                                }
                                if(("landlord").equals(iMmessage.getFromUserIdUserRole())){
                                    return new IMTenantAndLuId(iMmessage.getToUserId()+"_"+iMmessage.getLuId(),iMmessage.getToUserId(),iMmessage.getStoreContent(),"to");
                                }
                            }
                        }
                        return new IMTenantAndLuId();
                    }
                });


        Pattern<IMTenantAndLuId, IMTenantAndLuId> imPattern = Pattern.<IMTenantAndLuId>
                begin("begin").where(
                new IterativeCondition<IMTenantAndLuId >() {
                    @Override
                    public boolean filter(IMTenantAndLuId imTalkMsgIdAndTenant,Context<IMTenantAndLuId> ctx) {
                        return "from".equals(imTalkMsgIdAndTenant.getFromOrTo());
                    }
                })
                .followedBy("next")
                .where(new IterativeCondition<IMTenantAndLuId>() {
                    @Override
                    public boolean filter(IMTenantAndLuId imTalkMsgIdAndTenant,Context<IMTenantAndLuId> ctx) throws Exception {
                        return !StringUtil.isEmptyOrWhiteSpace(imTalkMsgIdAndTenant.getStoreContent());
                    }
                })
                .within(Time.minutes(2));

        PatternStream<IMTenantAndLuId> patternStream = CEP.pattern(
                iMmessageFilterDataStream.keyBy(IMTenantAndLuId::getTenantId_luid),
                imPattern);

        OutputTag<IMTenantAndLuId> imTiemoutOutput = new OutputTag<IMTenantAndLuId>("imTimeout") {};

        SingleOutputStreamOperator<IMTenantAndLuId> complexResult = patternStream.select(
                imTiemoutOutput,
                new PatternTimeoutFunction<IMTenantAndLuId, IMTenantAndLuId>() {
                    @Override
                    public IMTenantAndLuId timeout(Map<String, List<IMTenantAndLuId>> pattern, long timeoutTimestamp) throws Exception {
//                        System.out.println("说话没有回复个数-------------"+pattern.get("begin").size());
                        System.out.println(pattern.get("begin").get(0).getTenantId_luid()+pattern.get("begin").get(0).getStoreContent());
                        System.out.println(timeoutTimestamp);
                        return pattern.get("begin").get(0);
                    }
                },
                new PatternSelectFunction<IMTenantAndLuId, IMTenantAndLuId>() {
                    @Override
                    public IMTenantAndLuId select(Map<String, List<IMTenantAndLuId>> pattern) throws Exception {
//                        System.out.println("说话没有回复个数-------------"+pattern.get("next").size());
                        return pattern.get("next").get(0);
                    }
                }
        );


        DataStream<IMTenantAndLuId> timeoutResult = complexResult.getSideOutput(imTiemoutOutput);

        timeoutResult.addSink(new RichSinkFunction<IMTenantAndLuId>() {
            private static final long serialVersionUID = -4443175430371919407L;
            PreparedStatement ps;
            private Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                connection = JdbcUtil.getConnection();
                String sql = "select * from xz_marketing.bookorderstate where operator_id=? and luid=? order by operator_time desc limit 1";
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
            public void invoke(IMTenantAndLuId value, Context context) throws Exception {
                System.out.println(value);
                String[] s = value.getTenantId_luid().split("_");
                ps.setString(1, s[0]);
                ps.setString(2, s[1]);

                if(value!=null) {
                    if (!queryIsCreateOrder(ps.executeQuery())) {
                        System.out.println("推荐房源逻辑");
//                        String res = RecallUtil.sendPost(value.f0, value.f1, "XZH400");
                    } else {
                        System.out.println("查到了下单，不推荐");
                    }
                }
            }
        });

        env.execute("RecallCEPTest");
    }
}
