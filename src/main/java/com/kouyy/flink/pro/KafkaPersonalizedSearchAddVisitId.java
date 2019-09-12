package com.kouyy.flink.pro;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kouyy.flink.function.GsAddVisitIdWindowFunction;
import com.kouyy.flink.sinks.GsAddVisitIdRedisSink;
import com.kouyy.flink.utils.FlinkPropertyUtil;
import com.kouyy.flink.utils.StringUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 转发服务器日志同步kafka,topic:gio,增加用户唯一标识
 * @author kouyouyang
 * @date 2019-08-13 17:42
 */
public class KafkaPersonalizedSearchAddVisitId {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(30000);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new RocksDBStateBackend("hdfs:///user/hdfs/checkpoints/personalsearchKafkaAddVisitId"));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));
//        env.setParallelism(20);//测试环境设置，上线不设置

        Properties properties = FlinkPropertyUtil.getPropertiesFile("kafka_consumer.properties");
        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>("gio", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromLatest();
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);//检查点成功才提交offset

        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        DataStream<Tuple4<String,String, String,Long>> gioHouseDetailDataStream = kafkaStream.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        String t ="";
                        String n="";
                        String u="";
                        JSONObject var=null;
                        String userId ="";
                        String houseID_var ="";
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            t = jsonObject.getString("t");
                            u = jsonObject.getString("u");
                            n = jsonObject.getString("n");
                            var = jsonObject.getJSONObject("var");
                        }catch (Exception e){
                            System.err.println("异常::"+e.getMessage()+"------埋点信息::"+value);
                        }
                        if(var!=null){
                             userId = var.getString("userId");
                             houseID_var = var.getString("houseID_var");
                        }
                        if ("cstm".equals(t)
                                && "houseDeatilPageView".equals(n)
                                && var!=null
                                && !StringUtil.isEmptyOrWhiteSpace(houseID_var)
                                && !StringUtil.isEmptyOrWhiteSpace(u)) {
                            return true;
                        }
                        return false;
                    }
                }).map(
                new MapFunction<String, Tuple4<String,String, String,Long>>() {
                    @Override
                    public Tuple4<String,String, String,Long> map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        long tm = Long.parseLong(jsonObject.getString("tm"));
                        JSONObject var = jsonObject.getJSONObject("var");
                        String houseID_var = var.getString("houseID_var");
                        String userId = var.getString("userId");
                        String u = jsonObject.getString("u");
                        return new Tuple4<>(u,userId,houseID_var,tm);
                    }
                });

//        gioHouseDetailDataStream.print();//测试代码

        SingleOutputStreamOperator<String> timeWindowOut = gioHouseDetailDataStream
                .keyBy(t -> t.f0)
                .timeWindow(Time.minutes(5))
                .process(new GsAddVisitIdWindowFunction());

        timeWindowOut.addSink(new GsAddVisitIdRedisSink());

        env.execute("增加用户唯一标识消费kafka流生成个性化搜索5分钟特征");


    }
}
