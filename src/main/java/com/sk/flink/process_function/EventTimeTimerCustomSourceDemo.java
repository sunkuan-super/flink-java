package com.sk.flink.process_function;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/8 - 22:12
 */
public class EventTimeTimerCustomSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 获取源
        DataStreamSource<Event> source = env.addSource(new CustomSource());

        SingleOutputStreamOperator<Event> watermarkSource =
                source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).
                        withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));


        watermarkSource
                .keyBy(event -> event.userID)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        Long timestamp = ctx.timestamp();
                        out.collect(ctx.getCurrentKey() + "数据到达，时间戳：" + new Timestamp(timestamp) + " watermark: " + ctx.timerService().currentWatermark());

                        // 设置一个10秒的定时器
                        ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect(ctx.getCurrentKey() + "定时器触发，触发时间：" + new Timestamp(timestamp) + " watermark: " + ctx.timerService().currentWatermark());
                    }
                }).print();

        env.execute();
    }

    /**
     *  -------------  (1)  ---------------
     *  ctx.collect(new Event("Mary", "product?=100", 1000L));
     *  Thread.sleep(5000);
     *  ctx.collect(new Event("Bob", "product?=200", 11000L));
     *  Thread.sleep(5000);
     *
     *  Mary数据到达，时间戳：1970-01-01 08:00:01.0 watermark: -9223372036854775808
     *  Bob数据到达，时间戳：1970-01-01 08:00:11.0 watermark: 999
     *  Mary定时器触发，触发时间：1970-01-01 08:00:11.0 watermark: 9223372036854775807
     *  Bob定时器触发，触发时间：1970-01-01 08:00:21.0 watermark: 9223372036854775807
     *
     *  当Mary的数据到达后，时间戳为08:00:01.0 水位线不会立即发生改变。水位线为长整型的最小值，紧接着watermark会更新为 999
     *  当Bob的数据到达后，时间戳为08:00:11.0 水位线不会立即发生改变，而是用的08:00:01.0对应的水位线 999，紧接着会更新为 08:00:10.999 同样也不会触发定时器
     *  当数据源结束的时候, Flink自动会将水位线推进到长整型的最大值，于是所有尚未触发的定时器这时就统一触发了，我们就在控制台上看到了后两个定时器触发的信息
     *
     *
     * -------------  (2)  ---------------
     *  ctx.collect(new Event("Mary", "product?=100", 1000L));
     *  Thread.sleep(5000);
     *  ctx.collect(new Event("Bob", "product?=200", 11000L));
     *  Thread.sleep(5000);
     *  ctx.collect(new Event("John", "product?=300", 11001L));
     *  Thread.sleep(5000);
     *
     *  Mary数据到达，时间戳：1970-01-01 08:00:01.0 watermark: -9223372036854775808
     *  Bob数据到达，时间戳：1970-01-01 08:00:11.0 watermark: 999
     *  John数据到达，时间戳：1970-01-01 08:00:11.001 watermark: 10999
     *  Mary定时器触发，触发时间：1970-01-01 08:00:11.0 watermark: 11000
     *  Bob定时器触发，触发时间：1970-01-01 08:00:21.0 watermark: 9223372036854775807
     *  John定时器触发，触发时间：1970-01-01 08:00:21.001 watermark: 9223372036854775807
     *
     *  当Mary的数据到达后，时间戳为08:00:01.0 水位线不会立即发生改变。水位线为长整型的最小值，紧接着watermark会更新为 999
     *  当Bob的数据到达后，时间戳为08:00:11.0 水位线不会立即发生改变，而是用的08:00:01.0对应的水位线 999，紧接着会更新为 08:00:10.999 同样也不会触发定时器
     *  当John的数据到达后，时间戳为08:00:11.001 水位线不会立即发生改变，而是用的08:00:11.0对应的水位线 10999，紧接着会更新为 08:00:11.0 此时该触发定时器了
     *  随即Mary定时器触发。当数据源结束的时候, Flink自动会将水位线推进到长整型的最大值，于是所有尚未触发的定时器这时就统一触发了，我们就在控制台上看到了后两个定时器触发的信息
     *
     */

    public static class CustomSource implements SourceFunction<Event> {

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            ctx.collect(new Event("Mary", "product?=100", 1000L));
            Thread.sleep(5000);
            ctx.collect(new Event("Bob", "product?=200", 11000L));
            Thread.sleep(5000);
            ctx.collect(new Event("John", "product?=300", 11001L));
            Thread.sleep(5000);
        }

        @Override
        public void cancel() {

        }
    }
}
