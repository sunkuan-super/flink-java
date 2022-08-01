package com.sk.flink.window;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceParallelDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description: 窗口类型
 * @date: 2022/8/1 - 22:20
 */
public class WindowReduceDemo {

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
        eventSingleOutputStreamOperator.map(
                event -> Tuple2.of(event.userID, 1L)
        ).returns(Types.TUPLE(Types.STRING, Types.LONG)).
                keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> tuple1, Tuple2<String, Long> tuple2) throws Exception {
                        return Tuple2.of(tuple1.f0, tuple1.f1 + tuple2.f1);
                    }
                }).print();

        env.execute();


    }
}
