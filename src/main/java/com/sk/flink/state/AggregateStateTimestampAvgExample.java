package com.sk.flink.state;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.util.Times;
import redis.clients.jedis.Tuple;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/22 - 23:15
 */
public class AggregateStateTimestampAvgExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new ClickSourceDemo())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long recordTimestamp) {
                                return event.timestamp;
                            }
                        }));

        streamOperator.print("data");

        // 自定义实现平均时间戳的统计
        streamOperator.keyBy(event -> event.userID)
                .flatMap(new AggregateTimestampResult(5L))
                .print();

        env.execute();
    }

    // 实现自定义的RichFlatMapFunction
    public static class AggregateTimestampResult extends RichFlatMapFunction<Event, String> {

        /**
         * 达到count个
         */
        private Long count;

        public AggregateTimestampResult(Long count) {
            this.count = count;
        }

        /**
         * 定义一个聚合的状态,用来保存平均时间戳
         */
        AggregatingState<Event, Long> aggregatingState;

        /**
         * 定义一个值状态,用来保存用户访问的次数
         */
        ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化聚合状态
            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "avg-ts",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)
            ));

            // 初始化值状态,保存用户的访问次数
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 每来一条数据，curr count 加1
            Long currCount = valueState.value();
            if (currCount == null) {
                currCount = 1L;
            } else {
                currCount++;
            }

            // 更新状态
            valueState.update(currCount);
            aggregatingState.add(value);

            // 如果达到count次数就输出结果
            if (currCount.equals(count)) {
                out.collect(value.userID + "过去" + count + "次访问的平均时间戳为： " + new Timestamp(aggregatingState.get()));

                // 清理状态
                valueState.clear();
                // 如果想单纯的用户访问5次就输出平均时间戳,而不是只输出最近5次的时间戳，可以不清理聚合状态
                // aggregatingState.clear();
            }
        }
    }
}
