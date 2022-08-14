package com.sk.flink.stream;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/11 - 22:09
 */
public class IntervalJoinStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 流1
        SingleOutputStreamOperator<Tuple2<String, Long>> orderStream = env.fromElements(
                Tuple2.of("Mary", 5000L),
                Tuple2.of("Bob", 5000L),
                Tuple2.of("James", 20000L),
                Tuple2.of("John", 51000L)
                //, Tuple2.of("b", 5100L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                        return tuple2.f1;
                    }
                })
        );

        // 流2
        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 5000),
                new Event("James", "prod?id=10", 15000),
                new Event("Bob", "./fav", 30000),
                new Event("John", "./home", 40000),
                new Event("John", "./cart", 30000)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                })
        );

        // TODO 只有keyStream才能使用intervalJoin
        orderStream.keyBy(tuple2 -> tuple2.f0)
                .intervalJoin(clickStream.keyBy(event -> event.userID))
                // 左边的是前5秒,右边的是后5秒
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> left, Event right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(right + " 可能导致了 " + left);
                    }
                }).print();

        env.execute();
    }
}
