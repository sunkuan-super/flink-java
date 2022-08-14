package com.sk.flink.stream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description: 窗口join
 * @date: 2022/8/11 - 21:03
 */
public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 流1
        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 1000L),
                Tuple2.of("b", 2000L)
                //, Tuple2.of("b", 5100L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                        return tuple2.f1;
                    }
                })
        );

        // 流2
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream2 = env.fromElements(
                Tuple2.of("a", 3000),
                Tuple2.of("a", 4000),
                Tuple2.of("b", 4500),
                Tuple2.of("b", 5500)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> tuple2, long l) {
                        return tuple2.f1;
                    }
                })
        );

        // 两条流做join生成的新类型为JoinedStreams。一个单独的流,不继承其他的流, 调用join方法的流为主流
        /**
         * public <T2> JoinedStreams<T, T2> join(DataStream<T2> otherStream) {
         *         return new JoinedStreams<>(this, otherStream);
         * }
         *
         * public class JoinedStreams<T1, T2> {
         *      public JoinedStreams(DataStream<T1> input1, DataStream<T2> input2) {
         *              this.input1 = requireNonNull(input1);
         *              this.input2 = requireNonNull(input2);
         *      }
         * }
         */

        stream1.join(stream2)
                // 传入KeySelector<T1, KEY> keySelector, 返回Where<KEY>
                .where(data -> data.f0)
                // 传入KeySelector<T2, KEY> keySelector, 返回EqualTo
                .equalTo(data -> data.f0)
                // 传入WindowAssigner<? super TaggedUnion<T1, T2>, W> assigner
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Integer>, String>() {
                    @Override
                    public String join(Tuple2<String, Long> tuple1, Tuple2<String, Integer> tuple2) throws Exception {
                        return tuple1 + " -> " + tuple2;
                    }
                }).print();

        /**
         * 结果：
         * (a,1000) -> (a,3000)
         * (a,1000) -> (a,4000)
         * (a,2000) -> (a,3000)
         * (a,2000) -> (a,4000)
         * (b,1000) -> (b,4500)
         * (b,2000) -> (b,4500)
         *
         * (b,5000)在下一个窗口,所以没有能匹配上的,如果给stream1添加一个(b,5100)就能匹配上了
         */

        env.execute();
    }
}
