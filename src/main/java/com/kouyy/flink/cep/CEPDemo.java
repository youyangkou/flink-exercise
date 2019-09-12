package com.kouyy.flink.cep;

import com.kouyy.flink.utils.BeanConverter;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class CEPDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9000);

        Pattern<String, String> pattern = Pattern.<String>begin("first")
                .where(new SimpleCondition<String>() {
                    public boolean filter(String e) {
                        return e.contains("123");
                    }
                })
                .followedBy("second").where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.contains("456");
                    }
                })
                .within(Time.seconds(1));

        PatternStream<String> ps = CEP.pattern(dataStream, pattern);

        ps.select(new PatternTimeoutFunction<String, String>() {
                      @Override
                      public String timeout(Map<String, List<String>> pattern, long timeoutTimestamp) throws Exception {
                          // 没有3s内评价的
                          System.out.println("===============>>>>>>>>>> behaviors: " + BeanConverter.mapToJson(pattern));
                          return null;
                      }
                  },
                new PatternSelectFunction<String, String>() {
                    @Override
                    public String select(Map<String, List<String>> pattern) throws Exception {
                        // 3s内评价了的
                        System.out.println("===============>>>>>>>>>> behavior size: " + pattern.size());
                        System.out.println("===============>>>>>>>>>> behaviors: " + BeanConverter.mapToJson(pattern));
                        return "";
                    }
                });

        env.execute();
    }
}
