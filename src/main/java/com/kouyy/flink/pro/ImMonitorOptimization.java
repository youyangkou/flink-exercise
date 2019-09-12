package com.kouyy.flink.pro;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kouyy.flink.sinks.ImMonitorMysqlSinkA;
import com.kouyy.flink.sinks.ImMonitorMysqlSinkB;
import com.kouyy.flink.utils.FlinkPropertyUtil;
import com.kouyy.flink.utils.StringUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

/**
 *IM触发无房监控优化
 */
public class ImMonitorOptimization {

    static Map<String, String> pattern_map = FlinkPropertyUtil.getMapFromDB("select * from xz_data_monitor.xz_im_monitor_pattern", "pattern_key", "pattern_value");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(30000);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setStateBackend(new RocksDBStateBackend("hdfs:///user/hdfs/checkpoints/imMonitorOptimization"));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));

        Properties properties = FlinkPropertyUtil.getPropertiesFile("kafka_consumer.properties");

        /*SimpleStringSchema可以获取到kafka消息，JSONKeyValueDeserializationSchema可以获取都消息的key,value，metadata:topic,partition，offset等信息*/
//        FlinkKafkaConsumer010<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer010<>("rabbitmq-im", new JSONKeyValueDeserializationSchema(true), properties);
        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>("rabbitmq-im", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromLatest();
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);//检查点成功提交offset

        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        //talk_id为房客id_房东id_房源id

        DataStream<Tuple6<String,String,String,String,String,String>> iMDataStream  = kafkaStream.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        String fromUserId="";
                        String toUserId="";
                        String luId="";
                        String fromUserIdUserRole="";
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            fromUserId = jsonObject.getString("fromUserId");
                            toUserId = jsonObject.getString("toUserId");
                            luId = jsonObject.getString("luId");
                            fromUserIdUserRole = jsonObject.getString("fromUserIdUserRole");
                        }catch (Exception e){
                            System.err.println("异常::"+e.getMessage()+"------IM消息::"+value);
                        }
                        if(!StringUtil.isEmptyOrWhiteSpace(fromUserId)
                                &&!StringUtil.isEmptyOrWhiteSpace(toUserId)
                                &&!StringUtil.isEmptyOrWhiteSpace(fromUserIdUserRole)
                                &&!StringUtil.isEmptyOrWhiteSpace(luId)
                        ){
                            return true;
                        }
                        return false;
                    }
                }
        ).map(
                new MapFunction<String, Tuple6<String,String,String,String,String,String>>() {
                    @Override
                    public Tuple6<String,String,String,String,String,String> map(String value) throws Exception {
                        String storeContent ="";
                        String fromUserId="";
                        String toUserId="";
                        String luId="";
                        String fromUserIdUserRole="";
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            storeContent = jsonObject.getString("storeContent");
                            fromUserId = jsonObject.getString("fromUserId");
                            toUserId = jsonObject.getString("toUserId");
                            luId = jsonObject.getString("luId");
                            fromUserIdUserRole = jsonObject.getString("fromUserIdUserRole");
                        }catch (Exception e){
                            System.err.println("异常::"+e.getMessage()+"------IM消息::"+value);
                        }
                        if(fromUserIdUserRole.equals("landlord")){
                            return new Tuple6<String,String,String,String,String,String>(toUserId+"_"+fromUserId+"_"+luId,toUserId,fromUserId,"landlord",storeContent,value);
                        }else{
                            return new Tuple6<String,String,String,String,String,String>(fromUserId+"_"+toUserId+"_"+luId,fromUserId,toUserId,"tenant",storeContent,value);
                        }
                    }
                });

        Pattern<Tuple6<String,String,String,String,String,String>,Tuple6<String,String,String,String,String,String>> imPatternA =
                Pattern.<Tuple6<String,String,String,String,String,String>>
                        begin("begin").where(
                        new IterativeCondition<Tuple6<String,String,String,String,String,String>>() {
                            @Override
                            public boolean filter(Tuple6<String, String, String, String, String, String> value, Context<Tuple6<String, String, String, String, String, String>> ctx) throws Exception {
                                String tenant_pattern_a = pattern_map.get("tenant_pattern_A");
                                if(StringUtil.isEmptyOrWhiteSpace(tenant_pattern_a)){
                                    pattern_map = FlinkPropertyUtil.getMapFromDB("select * from xz_data_monitor.xz_im_monitor_pattern", "pattern_key", "pattern_value");
                                    tenant_pattern_a = pattern_map.get("tenant_pattern_A");
                                    System.out.println(tenant_pattern_a);
                                }
                                java.util.regex.Pattern p_ten = java.util.regex.Pattern.compile(tenant_pattern_a);
                                Matcher m_ten = p_ten.matcher(value.f4);
                                return  value.f3.equals("tenant")&&m_ten.find();
                            }
                        })
                        .next("next")
                        .where(new IterativeCondition<Tuple6<String,String,String,String,String,String>>() {
                            @Override
                            public boolean filter(Tuple6<String, String, String, String, String, String> value, Context<Tuple6<String, String, String, String, String, String>> ctx) throws Exception {
                                String landlord_pattern_1 = pattern_map.get("landlord_pattern_1");
                                String landlord_pattern_2 = pattern_map.get("landlord_pattern_2");
                                if(StringUtil.isEmptyOrWhiteSpace(landlord_pattern_1)||StringUtil.isEmptyOrWhiteSpace(landlord_pattern_2)){
                                    System.out.println("缓存过期了");
                                    pattern_map = FlinkPropertyUtil.getMapFromDB("select * from xz_data_monitor.xz_im_monitor_pattern", "pattern_key", "pattern_value");
                                    landlord_pattern_1 = pattern_map.get("landlord_pattern_1");
                                    landlord_pattern_2 = pattern_map.get("landlord_pattern_2");
                                    System.out.println(landlord_pattern_1);
                                    System.out.println(landlord_pattern_2);
                                }
                                java.util.regex.Pattern p_lan1 = java.util.regex.Pattern.compile(landlord_pattern_1);
                                java.util.regex.Pattern p_lan2 = java.util.regex.Pattern.compile(landlord_pattern_2);
                                Matcher m_ten1 = p_lan1.matcher(value.f4);
                                Matcher m_ten2 = p_lan2.matcher(value.f4);
                                if(value.f3.equals("landlord")){
                                    if(m_ten1.find()||m_ten2.find()){
                                        return true;
                                    }
                                }
                                return  false;
                            }
                        }).oneOrMore().consecutive();

        Pattern<Tuple6<String,String,String,String,String,String>,Tuple6<String,String,String,String,String,String>> imPatternB =
                Pattern.<Tuple6<String,String,String,String,String,String>>
                        begin("begin").where(
                        new IterativeCondition<Tuple6<String,String,String,String,String,String>>() {
                            @Override
                            public boolean filter(Tuple6<String, String, String, String, String, String> value, Context<Tuple6<String, String, String, String, String, String>> ctx) throws Exception {
                                String tenant_pattern_a = pattern_map.get("tenant_pattern_A");
                                String tenant_pattern_b = pattern_map.get("tenant_pattern_B");
                                if(StringUtil.isEmptyOrWhiteSpace(tenant_pattern_a)||StringUtil.isEmptyOrWhiteSpace(tenant_pattern_b)){
                                    System.out.println("缓存过期了");
                                    pattern_map = FlinkPropertyUtil.getMapFromDB("select * from xz_data_monitor.xz_im_monitor_pattern", "pattern_key", "pattern_value");
                                    tenant_pattern_a = pattern_map.get("tenant_pattern_A");
                                    tenant_pattern_b = pattern_map.get("tenant_pattern_B");
                                    System.out.println(tenant_pattern_a);
                                    System.out.println(tenant_pattern_b);
                                }
                                java.util.regex.Pattern p_ten1 = java.util.regex.Pattern.compile(tenant_pattern_a);
                                java.util.regex.Pattern p_ten2 = java.util.regex.Pattern.compile(tenant_pattern_b);

                                Matcher m_ten1 = p_ten1.matcher(value.f4);
                                Matcher m_ten2 = p_ten2.matcher(value.f4);

                                if(value.f3.equals("tenant")){
                                    if(!m_ten1.find()&&!m_ten2.find()){
                                        return true;
                                    }
                                }
                                return false;
                            }
                        })
                        .next("next")
                        .where(new IterativeCondition<Tuple6<String,String,String,String,String,String>>() {
                            @Override
                            public boolean filter(Tuple6<String, String, String, String, String, String> value, Context<Tuple6<String, String, String, String, String, String>> ctx) throws Exception {

                                String landlord_pattern_1 = pattern_map.get("landlord_pattern_1");
                                String landlord_pattern_2 = pattern_map.get("landlord_pattern_2");
                                if(StringUtil.isEmptyOrWhiteSpace(landlord_pattern_1)||StringUtil.isEmptyOrWhiteSpace(landlord_pattern_2)){
                                    System.out.println("缓存过期了");
                                    pattern_map = FlinkPropertyUtil.getMapFromDB("select * from xz_data_monitor.xz_im_monitor_pattern", "pattern_key", "pattern_value");
                                    landlord_pattern_1 = pattern_map.get("landlord_pattern_1");
                                    landlord_pattern_2 = pattern_map.get("landlord_pattern_2");
                                    System.out.println(landlord_pattern_1);
                                    System.out.println(landlord_pattern_2);
                                }
                                java.util.regex.Pattern p_lan1 = java.util.regex.Pattern.compile(landlord_pattern_1);
                                java.util.regex.Pattern p_lan2 = java.util.regex.Pattern.compile(landlord_pattern_2);
                                Matcher m_lan1 = p_lan1.matcher(value.f4);
                                Matcher m_lan2 = p_lan2.matcher(value.f4);
                                if(value.f3.equals("landlord")){
                                    if(m_lan1.find()||m_lan2.find()){
                                        return true;
                                    }
                                }
                                return  false;
                            }
                        }).oneOrMore().consecutive();


        PatternStream<Tuple6<String, String, String, String, String, String>> patternStreamA = CEP.pattern(iMDataStream.keyBy(t -> t.f0), imPatternA);
        PatternStream<Tuple6<String, String, String, String, String, String>> patternStreamB = CEP.pattern(iMDataStream.keyBy(t -> t.f0), imPatternB);

        DataStream<Tuple6<String, String, String, String, String, String>> resultA = patternStreamA.select(
                new PatternSelectFunction<Tuple6<String, String, String, String, String, String>,Tuple6<String, String, String, String, String, String>>() {
                    @Override
                    public Tuple6<String, String, String, String, String, String> select(Map<String, List<Tuple6<String, String, String, String, String, String>>> pattern) throws Exception {
                        System.out.println("策略A："+pattern.get("begin").get(0).f0+":"+pattern.get("begin").get(0).f4+"----"+pattern.get("next").get(0).f0+":"+pattern.get("next").get(0).f4);
                        return pattern.get("next").get(0);
                    }
                });

        DataStream<Tuple6<String, String, String, String, String, String>> resultB = patternStreamB.select(
                new PatternSelectFunction<Tuple6<String, String, String, String, String, String>,Tuple6<String, String, String, String, String, String>>() {
                    @Override
                    public Tuple6<String, String, String, String, String, String> select(Map<String, List<Tuple6<String, String, String, String, String, String>>> pattern) throws Exception {
                        System.out.println("策略B："+pattern.get("begin").get(0).f0+":"+pattern.get("begin").get(0).f4+"----"+pattern.get("next").get(0).f0+":"+pattern.get("next").get(0).f4);
                        return pattern.get("next").get(0);
                    }
                });

        resultA.addSink(new ImMonitorMysqlSinkA());
        resultB.addSink(new ImMonitorMysqlSinkB());

        env.execute("IM触发无房监控优化");


    }
}
