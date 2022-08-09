package com.sk.flink.process_function;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import com.sk.flink.source.ClickSourceParallelDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/8 - 22:12
 */
public class EventTimeTimerDemo {
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

        /**
         * John数据到达，时间戳：2022-08-08 22:18:13.417 watermark: -9223372036854775808
         * James数据到达，时间戳：2022-08-08 22:18:14.421 watermark: 1659968293416
         * John数据到达，时间戳：2022-08-08 22:18:15.424 watermark: 1659968294420
         * Bob数据到达，时间戳：2022-08-08 22:18:16.425 watermark: 1659968295423
         * James数据到达，时间戳：2022-08-08 22:18:17.427 watermark: 1659968296424
         * John数据到达，时间戳：2022-08-08 22:18:18.43 watermark: 1659968297426
         * John数据到达，时间戳：2022-08-08 22:18:19.433 watermark: 1659968298429
         * James数据到达，时间戳：2022-08-08 22:18:20.434 watermark: 1659968299432
         * James数据到达，时间戳：2022-08-08 22:18:21.435 watermark: 1659968300433
         * James数据到达，时间戳：2022-08-08 22:18:22.436 watermark: 1659968301434
         * Mary数据到达，时间戳：2022-08-08 22:18:23.439 watermark: 1659968302435
         * John定时器触发，触发时间：2022-08-08 22:18:23.417 watermark: 1659968303438
         * Mary数据到达，时间戳：2022-08-08 22:18:24.443 watermark: 1659968303438
         * James定时器触发，触发时间：2022-08-08 22:18:24.421 watermark: 1659968304442
         * John数据到达，时间戳：2022-08-08 22:18:25.446 watermark: 1659968304442
         *
         * 解释： 当2022-08-08 22:18:23.439数据到的时候并不能立即生成他对应的watermark，
         *      而是用的 2022-08-08 22:18:22.436 对应的watermark为 22:18:22.435, 此时不会触发定时器。
         *      紧接着随着时间的推移，watermark更新了，变为2022-08-08 22:18:23.439对应的2022-08-08 22:18:23.438
         *      则立马去触发定时的2022-08-08 22:18:23.417
         */

        env.execute();
    }
}
