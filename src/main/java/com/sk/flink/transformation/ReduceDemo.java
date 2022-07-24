package com.sk.flink.transformation;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/24 - 14:58
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> eventDataStreamSource = env.fromElements(
                new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 3000),
                new Event("James", "prod?id=10", 4000),
                new Event("Bob", "./fav", 3000),
                new Event("John", "./home", 4000),
                new Event("Bob", "./fav", 4000),
                new Event("John", "./cart", 5000)
        );

        SingleOutputStreamOperator<Tuple2<String, Long>> mapStream =
                eventDataStreamSource.map(data -> Tuple2.of(data.userID, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> keyedStream = mapStream.keyBy(data -> data.f0);
        // 求每一位用户的点击量
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = keyedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> tuple1, Tuple2<String, Long> tuple2) throws Exception {
                return Tuple2.of(tuple1.f0, tuple1.f1 + tuple2.f1);
            }
        });

        // 请点击量最大的用户 keyBy写一个固定的分区，代表只分一个区
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicksByUser.keyBy(tuple -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> tuple1, Tuple2<String, Long> tuple2) throws Exception {
                return tuple1.f1 > tuple2.f1 ? tuple1 : tuple2;
            }
        });

        result.print();

        env.execute();
    }
}
