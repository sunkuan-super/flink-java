package com.sk.flink.state;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description: MapState模拟滚动窗口Demo
 * @date: 2022/8/22 - 22:20
 */
public class MapStateFakeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 设置水位线和时间分配器
        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new ClickSourceDemo())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        streamOperator.print("source");

        streamOperator.keyBy(event -> event.urls)
                .process(new FakeWindowResult(10000L))
                .print();

        env.execute();
    }

    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {

        /**
         * 设置窗口大小
         */
        private Long windowSize;

        /**
         * 定义映射状态,Key表示窗口的起始时间,Value表示窗口中某个url的count值
         */
        private MapState<Long, Long> windowUrlCountMapState;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            windowUrlCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("windowUrlCountMapState", Long.class, Long.class));
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<String> out) throws Exception {
            Long windowStart = event.timestamp / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;

            // 以窗口开始为key
            if (windowUrlCountMapState.contains(windowStart)) {
                Long count = windowUrlCountMapState.get(windowStart);
                windowUrlCountMapState.put(windowStart, count + 1);
            } else {
                windowUrlCountMapState.put(windowStart, 1L);
            }

            // 设置的事件时间定时器都是通过Watermark触发的
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1L;
            Long windowStart = windowEnd - windowSize;
            String url = ctx.getCurrentKey();
            Long count = windowUrlCountMapState.get(windowStart);

            out.collect("窗口：" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd)
                    + " url: " + url +
                    " count: " + count
            );

            windowUrlCountMapState.remove(windowStart);
        }
    }
}
