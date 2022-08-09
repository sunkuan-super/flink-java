package com.sk.flink.process_function;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/8 - 21:22
 */
public class ProcessTimeTimerDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> source = env.addSource(new ClickSourceDemo());


        KeyedStream<Event, String> keyedStream = source.keyBy(event -> event.userID);
        // KeyedProcessFunction<K, I, O>  Key Input Output
        keyedStream.process(new KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                // 注意要用ctx.timerService().currentProcessingTime() 而不是 ctx.timestamp()
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                out.collect(ctx.getCurrentKey() + "数据到达，到达时间1：" + new Timestamp(currentProcessingTime));

                // 注册一个10秒后的定时器 (想注册一个定时器,必须是用KeyedStream)
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 10 * 1000L);
            }

            /**
             *
             * @param timestamp 定时器的时间戳
             * @param ctx OnTimerContext 继承了processElement中的第二个参数的Context
             * @param out 输出
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                out.collect(ctx.getCurrentKey() + "定时器触发，触发时间：" + new Timestamp(timestamp));
            }
        }).print();

        env.execute();
    }
}
