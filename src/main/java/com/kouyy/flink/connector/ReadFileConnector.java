package com.kouyy.flink.connector;


import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.concurrent.TimeUnit;

/**
 * 读取文件source
 */
public class ReadFileConnector {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(30000);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));

        DataStreamSource<String> dataStream = env.readTextFile("/Users/kouyouyang/Desktop/flink/gio-log.txt");

        //Sink有DataStream的writeAsText(path)和 writeAsCsv(path)
        dataStream.writeAsText("/Users/kouyouyang/Desktop/flink/gio-log-1.txt");

        env.execute("ReadFileConnector");


    }
}
