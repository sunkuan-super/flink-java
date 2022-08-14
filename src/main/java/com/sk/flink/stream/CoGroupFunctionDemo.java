package com.sk.flink.stream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/14 - 17:04
 */
public class CoGroupFunctionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 流1
        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 1000L),
                Tuple2.of("b", 2000L),
                Tuple2.of("c", 2000L)
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

        // coGroup是一个更加通用的Window Join,不仅可以做内连接还可以做外连接
        stream1.coGroup(stream2)
                .where(tuple2 -> tuple2.f0)
                .equalTo(tuple2 -> tuple2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Integer>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Long>> iterable, Iterable<Tuple2<String, Integer>> iterable1, Collector<String> collector) throws Exception {
                        // 分组后，每个key每个窗口只会调用一次
                        collector.collect(iterable + ", " + iterable1);
                        // 模拟实现左外连接
                        Iterator<Tuple2<String, Long>> iterator = iterable.iterator();
                        Iterator<Tuple2<String, Integer>> iterator1 = iterable1.iterator();
                        List<Tuple2<String, Long>> list1 = new ArrayList<>();
                        List<Tuple2<String, Integer>> list2 = new ArrayList<>();
                        while (iterator.hasNext()){
                            list1.add(iterator.next());
                        }

                        while (iterator1.hasNext()){
                            list2.add(iterator1.next());
                        }

                        if(list1.size() == 0){
                            for (Tuple2<String, Integer> tuple : list2) {
                                collector.collect("[] -> "  + tuple);
                            }
                        }else {
                            for (Tuple2<String, Long> tuple1 : list1) {
                                for (Tuple2<String, Integer> tuple2 : list2) {
                                    collector.collect( tuple1+ "->" + tuple2);
                                }
                            }
                        }
                    }
                }).print();

        env.execute();
    }
}
