package com.sk.flink.state;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/23 - 21:53
 */
public class StateTTLDemo {
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

        stream.keyBy(event -> event.userID)
                .flatMap(new MyFlatMap()).print();

        env.execute();
    }

    // 实现自定义的FlatMapFunction,用于KeyedState测试
    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {

        // 定义值状态
        ValueState<Event> myValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("myValueState", Event.class);
            myValueState = getRuntimeContext().getState(valueStateDescriptor);

            // 配置状态的TTL
            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    // 设置更新失效时间的类型, OnCreateAndWrite
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    // 设置状态的可见性，
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();

            valueStateDescriptor.enableTimeToLive(stateTtlConfig);


        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {

        }
    }
}
