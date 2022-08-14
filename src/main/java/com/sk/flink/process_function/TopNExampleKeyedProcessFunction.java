package com.sk.flink.process_function;

import com.sk.flink.bean.Event;
import com.sk.flink.bean.UrlViewCount;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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
public class TopNExampleKeyedProcessFunction {
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

        watermarkSource.print("source");

        SingleOutputStreamOperator<UrlViewCount> urlCountStream = watermarkSource.keyBy(event -> event.urls)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlCountAggregateFunction(), new UrlCountResult());

        urlCountStream.print("url count");

        urlCountStream.keyBy(urlViewCount -> urlViewCount.windowEnd)
                .process(new TopNKeyedProcessFunction(2))
                .print();

        env.execute();
    }

    /**
     * 自定义的增量聚合函数
     */
    public static class UrlCountAggregateFunction implements AggregateFunction<Event, Long, Long> {

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

    /**
     * 自定义全窗口函数
     */
    public static class UrlCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long count = elements.iterator().next();

            out.collect(new UrlViewCount(key, count, start, end));
        }
    }

    // KeyedProcessFunction<K, I, O>
    public static class TopNKeyedProcessFunction extends KeyedProcessFunction<Long, UrlViewCount, String> {

        /**
         * 定义一个属性topN
         */
        private Integer topN;

        /**
         * 定义列表状态
         */
        private ListState<UrlViewCount> urlViewCountListState;


        public TopNKeyedProcessFunction(Integer topN) {
            this.topN = topN;
        }

        /**
         * 在环境中存取状态
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化时候给状态赋值
            urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("url-count", Types.POJO(UrlViewCount.class)));
        }

        /**
         * 每来一个处理一次
         *
         * @param value 来的元素
         * @param ctx
         * @param out   输出
         * @throws Exception
         */
        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {

            // 将数据保存到状态中
            urlViewCountListState.add(value);

            // 注册window + 1的定时器
            Long currentKey = ctx.getCurrentKey();
            // 定时器都是由watermark触发的
            ctx.timerService().registerEventTimeTimer(currentKey + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            List<UrlViewCount> urlViewCounts = new ArrayList<>();

            // 从状态中取出数据
            Iterator<UrlViewCount> iterator = urlViewCountListState.get().iterator();
            while (iterator.hasNext()) {
                UrlViewCount urlViewCount = iterator.next();
                urlViewCounts.add(urlViewCount);
            }

            urlViewCounts.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return (int) (o2.count - o1.count);
                }
            });

            // 包装打印输出
            StringBuilder sb = new StringBuilder();
            sb.append("---------------------------------- \n");
            sb.append("窗口结束时间：" + new Timestamp(ctx.getCurrentKey()) + "\n");
            for (int i = 0; i < urlViewCounts.size(); i++) {
                if (i == 2) {
                    break;
                }

                UrlViewCount urlViewCount = urlViewCounts.get(i);
                String info = "No. " + (i + 1) +
                        " url: " + urlViewCount.url + " 访问量：" + urlViewCount.count + " \n";
                sb.append(info);
            }

            sb.append("---------------------------------- \n");

            out.collect(sb.toString());
        }
    }
}
