package com.sk.flink.window;

import com.sk.flink.bean.Event;
import com.sk.flink.bean.UrlViewCount;
import com.sk.flink.source.ClickSourceDemo;
import com.sk.flink.source.ClickSourceParallelDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/5 - 23:16
 */
public class UrlViewCountExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(1);
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

        // WindowFunction
        watermarkSource
                .keyBy(event -> event.urls)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 针对第一个参数(增量聚合函数)来处理窗口数据,每来一个数据就做一次聚合。这里的全窗口函数就不再缓存所有数据了
                // 等到窗口需要触发计算时, 则调用第二个参数(全窗口函数)的处理逻辑输出结果，第一个参数的输出作为第二个参数的输入
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
                .print();

        env.execute();
    }

    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String keyUrl, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long urlCount = elements.iterator().next();

            out.collect(new UrlViewCount(keyUrl, urlCount, start, end));
        }
    }
}
