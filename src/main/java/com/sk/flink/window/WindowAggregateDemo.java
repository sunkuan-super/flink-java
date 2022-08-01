package com.sk.flink.window;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceParallelDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description: 窗口类型
 * @date: 2022/8/1 - 22:20
 */
public class WindowAggregateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<Event> eventDataStreamSource = env.fromElements(
//                new Event("Mary", "./home", 1000),
//                new Event("Bob", "./cart", 2000),
//                new Event("John", "./fav", 5000),
//                new Event("James", "prod?id=10", 4000),
//                new Event("Bob", "./fav", 3000),
//                new Event("John", "./home", 4000),
//                new Event("John", "./cart", 3000)
//        );
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSourceParallelDemo());

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = eventDataStreamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {

                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        /**
         * keyBy后开窗，有window()方法和countWindow()方法,返回的是WindowedStream
         *
         * WindowedStream并不属于DataStream，需要调用聚合函数才能转换成DataStream
         */
        // 计算用户访问时间戳的平均数
        eventSingleOutputStreamOperator.
                keyBy(data -> data.userID)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, String>() {

                    /**
                     * 定义初始化累加器
                     * 类似于富函数中的open()方法 初始化时候执行一次
                     * @return
                     */
                    @Override
                    public Tuple2<Long, Integer> createAccumulator() {
                        return Tuple2.of(0L, 0);
                    }

                    /**
                     * 计算的结果与累加器保持一致
                     * @param event
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> accumulator) {
                        return Tuple2.of(event.timestamp + accumulator.f0, accumulator.f1 + 1);
                    }

                    @Override
                    public String getResult(Tuple2<Long, Integer> accumulator) {
                        Timestamp timestamp = new Timestamp(accumulator.f0 / accumulator.f1);
                        return timestamp.toString();
                    }

                    @Override
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> longIntegerTuple2, Tuple2<Long, Integer> acc1) {
                        return null;
                    }
                })
                .print();

        env.execute();
    }
}
