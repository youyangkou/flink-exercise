package com.kouyy.flink.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.Properties;

/**
 * @author kouyouyang
 * @date 2019-08-06 17:38
 */
public class EventTimeTest {
    public static void main(String[] args) {
        //生成流式执行环境对象 StreamExecutionEnvironment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableSysoutLogging();//开启Sysout打日志
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); //设置窗口的时间单位为process time
        env.setParallelism(2);//全局并发数
        //配置kafka bootstrap.servers
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka bootstrap.servers");
        //配置消息主题和应用名（自定义工具类FlinkKafkaManager，源码在后面）
        FlinkKafkaManager manager = new FlinkKafkaManager("kafka.topic", "app.name", properties);
        //用JsonObject 反序列化接收kafka
        FlinkKafkaConsumer09<JSONObject> consumer = manager.build(JSONObject.class);
        //从最新的消息开始接收
        consumer.setStartFromLatest();
        //获得DataStream
        DataStream<JSONObject> messageStream = env.addSource(consumer);
        //转化为pojo
        DataStream<Bean3> bean3DataStream = messageStream.map(new FlatMap());
        bean3DataStream
                .keyBy(Bean3::getAppId) //也可以用“appId”替换
                .timeWindow(Time.seconds(10))//等价于下面这一行，因为上面设置了TimeCharacteristic.ProcessingTime
                // .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))//基于process time的窗口
                .allowedLateness(Time.seconds(0)) //允许数据延迟多长时间,谨慎使用,迟到的数据会导致出现重复数据
                .aggregate(new Agg()) //聚合函数，这里也可以参照demo2用reduce函数
                .addSink(new Sink()); //输出函数
        try {
            env.execute("app.name");//流式处理需要调用触发
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class FlatMap implements MapFunction<JSONObject, Bean3> {
        @Override
        public Bean3 map(JSONObject jsonObject) throws Exception {
            return new Bean3(jsonObject.getString("appId"), jsonObject.getString("module"));
        }
    }

    public static class Agg implements AggregateFunction<Bean3, Tuple2<Bean3, Long>, Tuple2<Bean3, Long>> {
        @Override
        public Tuple2<Bean3, Long> createAccumulator() {
            return new Tuple2<Bean3, Long>();
        }

        @Override
        public Tuple2<Bean3, Long> add(Bean3 bean3, Tuple2<Bean3, Long> bean3LongTuple2) {
            Bean3 bean = bean3LongTuple2.f0;
            Long count = bean3LongTuple2.f1;
            if (bean == null) {
                bean = bean3;
            }
            if (count == null) {
                count = 1L;
            } else {
                count++;
            }
            return new Tuple2<>(bean, count);
        }

        @Override
        public Tuple2<Bean3, Long> getResult(Tuple2<Bean3, Long> bean3LongTuple2) {
            return bean3LongTuple2;
        }

        @Override
        public Tuple2<Bean3, Long> merge(Tuple2<Bean3, Long> bean3LongTuple2, Tuple2<Bean3, Long> acc1) {
            Bean3 bean = bean3LongTuple2.f0;
            Long count = bean3LongTuple2.f1;
            Long acc = acc1.f1;
            return new Tuple2<>(bean, count + acc);
        }
    }

    public static class Sink implements SinkFunction<Tuple2<Bean3, Long>> {
        @Override
        public void invoke(Tuple2<Bean3, Long> value, Context context) throws Exception {
            System.out.println(value.f0.toString() + "," + value.f1);
        }
    }

    public static class Bean3 {
        public String appId;
        public String module;

        public Bean3() {
        }

        public Bean3(String appId, String module) {
            this.appId = appId;
            this.module = module;
        }

        public String getAppId() {
            return appId;
        }

        public void setAppId(String appId) {
            this.appId = appId;
        }

        public String getModule() {
            return module;
        }

        public void setModule(String module) {
            this.module = module;
        }

        @Override
        public String toString() {
            return "Bean3{" +
                    "appId='" + appId + '\'' +
                    ", module='" + module + '\'' +
                    '}';
        }
    }
}
