package com.kouyy.flink.pro;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kouyy.flink.utils.FlinkPropertyUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author kouyouyang
 * @date 2019-09-06 11:23
 */
public class KafkaStreamSlect {

    private final static Logger logger = LoggerFactory.getLogger(KafkaStreamSlect.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(30000);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setStateBackend(new RocksDBStateBackend("hdfs:///user/hdfs/checkpoints/personalsearchKafkaAddVisitId"));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));
//        env.setParallelism(20);//测试环境设置，上线不设置

        Properties properties = FlinkPropertyUtil.getPropertiesFile("kafka_consumer.properties");

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>("gio", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromLatest();
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);//检查点成功才提交offset

        SplitStream<String> splitStream = env.addSource(kafkaConsumer)
                .name("select_source")
                .uid("uid_select_source")
                .split(new OutputSelector<String>() {
                    @Override
                    public Iterable<String> select(String value) {
                        List<String> tags = new ArrayList<>();
                        String t = "";
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            t = jsonObject.getString("t");
                        } catch (Exception e) {
                            logger.error("Json解析异常,json为{}", value);
                        }
                        switch (t) {
                            case "cstm":
                                tags.add(t);
                                break;
                            case "pvar":
                                tags.add(t);
                                break;
                            case "imp":
                                tags.add(t);
                                break;
                            case "page":
                                tags.add(t);
                                break;
                            case "clck":
                                tags.add(t);
                                break;
                        }
                        return tags;
                    }
                });

        DataStream<String> cstmStream = splitStream.select("cstm");
        DataStream<String> pvarStream = splitStream.select("pvar");
        DataStream<String> impStream = splitStream.select("imp");
        DataStream<String> clckStream = splitStream.select("clck");

//        FlinkKafkaProducer011<String> cstmProducer = new FlinkKafkaProducer011<String>(
//                "kafka00:9092,kafka01:9092,kafka02:9092",            // broker list
//                "cstm",                  // target topic
//                new SimpleStringSchema());

        FlinkKafkaProducer011<String> pvarProducer = new FlinkKafkaProducer011<String>(
                "kafka00:9092,kafka01:9092,kafka02:9092",            // broker list
                "pvar",                  // target topic
                new SimpleStringSchema());

//        cstmProducer.setWriteTimestampToKafka(true);
        pvarProducer.setWriteTimestampToKafka(true);

        cstmStream.addSink(pvarProducer);

        env.execute("分流测试");


    }
}
