package com.sk.flink.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: create by sunkuan
 * @Description: 实现笛卡尔积
 * @date: 2022/8/17 - 22:09
 */
public class TwoJoinByListStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("a", "stream1", 1000L),
                Tuple3.of("b", "stream1", 2000L),
                Tuple3.of("a", "stream1", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("a", "stream2", 3000L),
                Tuple3.of("b", "stream2", 4000L),
                Tuple3.of("a", "stream1", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        stream1.connect(stream2)
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {

                    ListState<Tuple3<String, String, Long>> stream1ListState;

                    ListState<Tuple3<String, String, Long>> stream2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stream1ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("list-state1", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                        stream2ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("list-state2", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, Context ctx, Collector<String> out) throws Exception {

                        for (Tuple3<String, String, Long> right : stream2ListState.get()) {
                            out.collect(left + " => " + right);
                        }

                        stream1ListState.add(left);
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                        for (Tuple3<String, String, Long> left : stream1ListState.get()) {
                            out.collect(left + " => " + right);
                        }

                        stream2ListState.add(right);
                    }
                }).print();

        env.execute();

    }
}
