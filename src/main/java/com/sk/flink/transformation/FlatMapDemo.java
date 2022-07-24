package com.sk.flink.transformation;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/23 - 19:07
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> dataStreamSource = env.fromElements(new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 3000),
                new Event("James", "prod?id=10", 4000)
        );

        // 1、自定义类实现FlatMapFunction接口
        SingleOutputStreamOperator<String> result1 = dataStreamSource.flatMap(new MyFlatMapFunction());
        result1.print("1");

        // 2、Lambda表达式 必须指定返回值，jvm有泛型擦除
        SingleOutputStreamOperator<String> result2 = dataStreamSource.flatMap((Event event, Collector<String> collector) -> {
            if (event.userID.equals("Mary")) {
                collector.collect(event.userID);
            } else if (event.userID.equals("Bob")) {
                collector.collect(event.userID);
                collector.collect(event.urls);
                collector.collect(event.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {
            @Override
            public TypeInformation<String> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
        //.returns(String.class);
        //.returns(Types.STRING);
        // 3种return的类型

        result2.print("2");
        env.execute();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.userID);
            collector.collect(event.urls);
            collector.collect(event.timestamp.toString());
        }
    }
}