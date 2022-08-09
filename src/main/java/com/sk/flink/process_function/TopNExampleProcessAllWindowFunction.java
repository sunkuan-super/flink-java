package com.sk.flink.process_function;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/9 - 21:07
 */
public class TopNExampleProcessAllWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

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

        watermarkSource.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlCountAggregateFunction(), new UrlTopNResult())
                .print();

        env.execute();
    }

    /**
     * 自定义的增量聚合函数
     */
    public static class UrlCountAggregateFunction implements AggregateFunction<Event, Map<String, Long>, List<Tuple2<String, Long>>> {

        @Override
        public Map<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, Long> add(Event event, Map<String, Long> accumulator) {
            String url = event.urls;
            if (accumulator.containsKey(url)) {
                Long count = accumulator.get(url);
                accumulator.put(url, count + 1);
            } else {
                accumulator.put(url, 1L);
            }

            return accumulator;
        }

        @Override
        public List<Tuple2<String, Long>> getResult(Map<String, Long> accumulator) {
            ArrayList<Tuple2<String, Long>> result = new ArrayList<>();
            Iterator<Map.Entry<String, Long>> iterator = accumulator.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Long> entry = iterator.next();
                result.add(Tuple2.of(entry.getKey(), entry.getValue()));
            }

            result.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.intValue() - o1.f1.intValue();
                }
            });
            return result;
        }

        @Override
        public Map<String, Long> merge(Map<String, Long> stringLongHashMap, Map<String, Long> acc1) {
            return null;
        }
    }

    /**
     * 自定义的全窗口函数
     */
    public static class UrlTopNResult extends ProcessAllWindowFunction<List<Tuple2<String, Long>>, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<List<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
            List<Tuple2<String, Long>> result = elements.iterator().next();

            StringBuilder sb = new StringBuilder();
            sb.append("---------------------------------\n");
            sb.append("窗口结束时间：").append(new Timestamp(context.window().getEnd())).append("\n");
            for (int i = 0; i < result.size(); i++) {
                if (i == 2) {
                    break;
                }

                String info = "No. " + (i + 1) +
                        " url: " + result.get(i).f0 + " 访问量：" + result.get(i).f1 + " \n";

                sb.append(info);
            }

            sb.append("---------------------------------\n");

            out.collect(sb.toString());
        }
    }
}
