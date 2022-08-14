package com.sk.flink.state;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description: 通过ValueState实现周期性输出pv,而不仅仅限于一个窗口中的pv或者开一个全天的窗
 * @date: 2022/8/14 - 22:08
 */
public class PeriodicPvExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSourceDemo())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.print("stream");

        stream.keyBy(event -> event.userID)
                .process(new PeriodicPvResult())
                .print();

        env.execute();
    }

    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {

        ValueState<Long> countState;

        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState", Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            Long count = countState.value();
            countState.update(count == null ? 1 : count + 1);

            if (timerTsState.value() == null) {
                // 初次当前key的第一个元素来的时候会注册定时器, 如果删除onTimer窗口中注册定时器的逻辑的话,有点像会话窗口,当上一个元素在onTimer
                // 中清除定时器后,并没有立马注册定时器,而是等再有下个元素到来的时候才开始设置的定时器,不像是滚动窗口一样,窗口的长度是一样的
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                timerTsState.update(value.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + " 's pv count: " + countState.value());
           //timerTsState.clear();

            // 加如下两行可以将定时器状态清理完继续创建定时器，有点像滚动窗口的样子,同时上边的timerTsState.clear() 可以不用
            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timerTsState.update(timestamp + 10 * 1000L);
        }
    }
}
