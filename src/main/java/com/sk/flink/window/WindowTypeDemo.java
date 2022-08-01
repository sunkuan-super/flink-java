package com.sk.flink.window;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description: 窗口类型
 * @date: 2022/8/1 - 22:20
 */
public class WindowTypeDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> eventDataStreamSource = env.fromElements(
                new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 5000),
                new Event("James", "prod?id=10", 4000),
                new Event("Bob", "./fav", 3000),
                new Event("John", "./home", 4000),
                new Event("John", "./cart", 3000)
        );

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
        WindowedStream<Event, String, TimeWindow> window = eventSingleOutputStreamOperator.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.getUserID();
            }
        })
                .window(TumblingEventTimeWindows.of(Time.hours(10)));// of一个参数的方法，普通的滚动事件时间窗口
// of两个参数的方法，第二个参数表示的是偏移量，正常的1小时的窗都是整点的，比如8:00到9:00，延迟5秒指的是8:05和9:05。或者用作有时差的情况
//                .window(TumblingEventTimeWindows.of(Time.hours(10), Time.minutes(5)))
                // of两个参数的方法，普通的华东事件时间窗口，第一个参数是窗口大小，第二个参数是滑动步长
//                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
                // of三个参数的方法，第三个参数是偏移量。与滚动窗口一样
//                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10), Time.seconds(4)))
                // 事件窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
                // 10个数开一个窗，滚动的计数窗口
//                .countWindow(10)
                // 10个数开一个窗，每两个数滑一步
//                .countWindow(10, 2)

        /**
         * 不keyBy直接开窗, 有windowAll()方法 返回的是WindowedStream
         *
         * WindowedStream并不属于DataStream
         */
        AllWindowedStream<Event, TimeWindow> eventTimeWindowAllWindowedStream =
                eventSingleOutputStreamOperator.windowAll(TumblingEventTimeWindows.of(Time.hours(1)));
    }
}
