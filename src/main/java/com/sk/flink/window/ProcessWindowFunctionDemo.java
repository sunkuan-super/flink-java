package com.sk.flink.window;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import com.sk.flink.source.ClickSourceParallelDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @author: create by sunkuan
 * @Description: 全窗口函数 WindowFunction
 * @date: 2022/8/5 - 22:00
 */
public class ProcessWindowFunctionDemo {

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
                .keyBy(event -> event.userID)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .process(new UVFunction())
                .print();

        env.execute();
    }

    /**
     * 自定义窗口处理函数
     */
    // ProcessWindowFunction<IN, OUT, KEY, W extends Window>
    public static class UVFunction extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        // process(KEY key, Context context, Iterable<IN> elements, Collector<OUT> out)
        @Override
        public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            HashSet<String> set = new HashSet<>();
            // 遍历所有数据放入set里去重
            for (Event element : elements) {
                set.add(element.userID);
            }

            int uv = set.size();

            // 窗口开始时间
            long start = context.window().getStart();
            // 窗口结束时间
            long end = context.window().getEnd();
            long maxTimestamp = context.window().maxTimestamp();

            int indexOfThisSubtask = this.getRuntimeContext().getIndexOfThisSubtask();

            // 结合窗口信息，包装输出内容 key分组分为几个，就有几个窗口
            out.collect("子任务为：" + indexOfThisSubtask + "窗口的开始时间为：" + new Timestamp(start) + ", 窗口的结束时间为：" + new Timestamp(end) + ", " +
                    "最大时间为：" + new Timestamp(maxTimestamp) + ", uv = " + uv);
        }
    }
}
