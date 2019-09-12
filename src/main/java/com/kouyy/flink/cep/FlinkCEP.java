package com.kouyy.flink.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FlinkCEP {

        private static final Logger LOGGER = LoggerFactory.getLogger(FlinkCEP.class);

        public static class DataSource implements Iterator<OrderEvent>, Serializable {

            private final AtomicInteger atomicInteger = new AtomicInteger(0);

            private final List<OrderEvent> orderEventList = Arrays.asList(
                    new OrderEvent("1", "create"),
                    new OrderEvent("2", "create"),
                    new OrderEvent("2", "pay")
            );

            @Override
            public boolean hasNext() {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return true;
            }

            @Override
            public OrderEvent next() {
                return orderEventList.get(atomicInteger.getAndIncrement() % 3);
            }
        }

        public static void main(String[] args) throws Exception {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //创建一个事件流
            DataStream<OrderEvent> loginEventStream = env.fromCollection(new DataSource(), OrderEvent.class);

            //以下表示如果在1秒钟内创建定单并付款则完成购物操作
            Pattern<OrderEvent, OrderEvent> loginFailPattern = Pattern.<OrderEvent>
                    begin("begin").where(
                  new IterativeCondition<OrderEvent>() {
                        @Override
                        public boolean filter(OrderEvent loginEvent, Context context) throws Exception {
                            return loginEvent.getType().equals("create");
                        }
                    })
                    .next("next")
                    .where(new IterativeCondition<OrderEvent>() {
                        @Override
                        public boolean filter(OrderEvent loginEvent, Context context) throws Exception {
                            return loginEvent.getType().equals("pay");
                        }
                    })
                    .within(Time.seconds(1));

            //把定单流应用到匹配流程中
            PatternStream<OrderEvent> patternStream = CEP.pattern(
                    loginEventStream.keyBy(OrderEvent::getUserId),
                    loginFailPattern);

            //侧出流
            OutputTag<OrderEvent> orderTiemoutOutput = new OutputTag<OrderEvent>("orderTimeout") {};

            //将正常定单流与侧超时流分开
            SingleOutputStreamOperator<OrderEvent> complexResult = patternStream.select(
                    orderTiemoutOutput,
                    (PatternTimeoutFunction<OrderEvent, OrderEvent>) (map, l) -> new OrderEvent("timeout", map.get("begin").get(0).getUserId()),
                    (PatternSelectFunction<OrderEvent, OrderEvent>) map -> new OrderEvent("success", map.get("next").get(0).getUserId())
            );

            DataStream<OrderEvent> timeoutResult = complexResult.getSideOutput(orderTiemoutOutput);

            complexResult.print();
            timeoutResult.print();

            env.execute();

        }

    }

class OrderEvent implements Serializable {

    private String userId;
    private String type;

    public OrderEvent(String userId, String type) {
        this.userId = userId;
        this.type = type;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OrderEvent)) return false;
        OrderEvent that = (OrderEvent) o;
        return Objects.equals(getUserId(), that.getUserId()) &&
                Objects.equals(getType(), that.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getUserId(), getType());
    }
}
