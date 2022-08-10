package com.sk.flink.stream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import javax.lang.model.element.ElementVisitor;
import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/10 - 22:22
 */
public class BillCheckExampleConnectStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 来自app的支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO).
                withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> tuple3, long l) {
                        return tuple3.f2;
                    }
                }));


        // 来自第三方支付平台的支付日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thridpartyStream = env.fromElements(
                Tuple4.of("order-1", "thrid-party", "sucess", 3000L),
                Tuple4.of("order-3", "thrid-party", "sucess", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO).
                withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> tuple4, long l) {
                        return tuple4.f3;
                    }
                }));

        // 两条流做connect 方式一： 两条流连接之后统一做keyBy
        ConnectedStreams<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>> appThridpartyConnectedStreams = appStream.connect(thridpartyStream).keyBy(tup1 -> tup1.f0, tup2 -> tup2.f0);
        // 两条流做connect 方式二： 两条流分别keyBy, 然后再做连接
        appStream.keyBy(tp -> tp.f0)
                .connect(thridpartyStream.keyBy(tp -> tp.f0));

        appThridpartyConnectedStreams.process(new MyCoProcessFunction()).print();


        env.execute();
    }

    public static class MyCoProcessFunction extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

        /**
         * 定义appEvent状态,因为是keyBy后,则状态只针对当前key
         */
        private ValueState<Tuple3<String, String, Long>> appEventState;

        /**
         * 定义thridpartyEvent状态,因为是keyBy后,则状态只针对当前key
         */
        private ValueState<Tuple4<String, String, String, Long>> thridpartyEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 给app流初始化状态
            appEventState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("appState", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));

            // 给第三方支付流初始化状态
            thridpartyEventState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thridpartyState", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            if (thridpartyEventState.value() != null) {
                // 第三方支付的数据到了，app支付的数据也到了，匹配成功
                out.collect("对账成功 " + value + " " + thridpartyEventState.value());
                // 既然匹配成功，则清除第三方支付的状态
                thridpartyEventState.clear();
            } else {
                // 第三方支付的数据没到，app支付的数据到了, 更新app支付的状态，定一个时钟去等待第三方支付的信息
                appEventState.update(value);
                // 注册一个定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            if(appEventState.value() != null){
                // app的数据到了，第三方支付的数据也到了,匹配成功
                out.collect("对账成功： " + appEventState.value() + " " + value);
                // 既然配置成功，则清除app的状态
                appEventState.clear();
            }else {
                // 第三方支付的数据到了， app的数据没到， 更新第三方支付的状态，定一个定时器去等待app支付的信息
                thridpartyEventState.update(value);
                // 注册一个定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f3);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 假设有没匹配上的情况,两个状态都不为空,肯定有个先后顺序,则一定会匹配成功,此时两个状态都清空了。
            // 如果匹配上了,那两个状态都为空
            // 如上两种状态都为空, 得出的结论：只要有状态不为空的,则就是没有匹配上
            if(appEventState.value() != null){
                out.collect("对账失败：" + appEventState.value() + " " + "第三方支付平台信息未到");
            }

            if(thridpartyEventState.value() != null){
                out.collect("对账失败: " + thridpartyEventState.value() + " " + "app数据未到" );
            }

            // 清理状态
            appEventState.clear();
            thridpartyEventState.clear();
        }
    }
}
