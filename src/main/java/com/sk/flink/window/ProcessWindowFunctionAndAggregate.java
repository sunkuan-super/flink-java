package com.sk.flink.window;

import com.sk.flink.bean.Event;
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
import java.util.HashSet;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/5 - 22:39
 */
public class ProcessWindowFunctionAndAggregate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(1);
        // 设置生成Watermark的周期性
        env.getConfig().setAutoWatermarkInterval(100);
        // 获取源
        DataStreamSource<Event> source = env.addSource(new ClickSourceParallelDemo());

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
                .keyBy(event -> "1")
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                // 针对第一个参数(增量聚合函数)来处理窗口数据,每来一个数据就做一次聚合。这里的全窗口函数就不再缓存所有数据了
                // 等到窗口需要触发计算时, 则调用第二个参数(全窗口函数)的处理逻辑输出结果，第一个参数的输出作为第二个参数的输入
                .aggregate(new UVAgg(), new ProcessWindow())
                .print();

        env.execute();
    }

    public static class UVAgg implements AggregateFunction<Event, HashSet<String>, Long> {

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> userSet) {
            userSet.add(event.userID);
            return userSet;
        }

        @Override
        public Long getResult(HashSet<String> userSet) {
            return (long) userSet.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }

    // <IN, OUT, KEY, W extends Window>
    public static class ProcessWindow extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        //  KEY key, Context context, Iterable<IN> elements, Collector<OUT> out
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();

            Long uv = elements.iterator().next();

            out.collect("窗口开始时间：" + start + ", 窗口结束时间：" + end + ", uv = " + uv);
        }
    }
}
