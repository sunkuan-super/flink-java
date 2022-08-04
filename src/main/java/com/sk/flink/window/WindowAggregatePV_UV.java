package com.sk.flink.window;

import com.sk.flink.bean.AAA;
import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import com.sk.flink.source.ClickSourceParallelDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/4 - 22:07
 */
public class WindowAggregatePV_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 设置生成Watermark的周期性
        env.getConfig().setAutoWatermarkInterval(100);
        // 获取源
        DataStreamSource<Event> source = env.addSource(new ClickSourceDemo());

        SingleOutputStreamOperator<Event> watermarkSource =
                source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).
                        withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));


        watermarkSource.print("data");

        watermarkSource
                .keyBy(event -> "1")
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AggregatePV_UV()).print();

        env.execute();
    }

    public static class AggregatePV_UV implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {

        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            // 创建累加器，一个窗口执行一次
            System.out.println("createAccumulator");
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            longHashSetTuple2.f1.add(event.userID);
            return Tuple2.of(longHashSetTuple2.f0 + 1, longHashSetTuple2.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            System.out.println("getResult");
            return (double) longHashSetTuple2.f0 / longHashSetTuple2.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }

//    public static class RichAggregatePV_UV extends RichAggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {
//
//        @Override
//        public Tuple2<Long, HashSet<String>> createAccumulator() {
//            // 创建累加器，一个窗口执行一次
//            System.out.println("createAccumulator");
//            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
//            System.out.println("第" + indexOfThisSubtask + "个并行子任务 createAccumulator");
//            return Tuple2.of(0L, new HashSet<>());
//        }
//
//        @Override
//        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> longHashSetTuple2) {
//            longHashSetTuple2.f1.add(event.userID);
//            return Tuple2.of(longHashSetTuple2.f0 + 1, longHashSetTuple2.f1);
//        }
//
//        @Override
//        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
//            System.out.println("getResult");
//            return (double) longHashSetTuple2.f0 / longHashSetTuple2.f1.size();
//        }
//
//        @Override
//        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
//            return null;
//        }
//    }
}
