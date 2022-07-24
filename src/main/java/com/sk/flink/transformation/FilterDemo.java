package com.sk.flink.transformation;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/23 - 18:48
 */
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> dataStreamSource = env.fromElements(new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 3000),
                new Event("James", "prod?id=10", 4000)
        );

        // 1、自定义类实现FilterFunction接口,filter方法中传入自定义类的对象
        SingleOutputStreamOperator<Event> result1 = dataStreamSource.filter(new MyFilterFunction());
        result1.print("result1: ");

        // 2、传入匿名内部类
        SingleOutputStreamOperator<Event> result2 = dataStreamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.userID.equals("Mary");
            }
        });
        result2.print("result2: ");

        SingleOutputStreamOperator<Event> result3 = dataStreamSource.filter(event -> event.userID.equals("Bob"));
        result3.print("result3: ");

        env.execute();
    }

    public static class MyFilterFunction implements FilterFunction<Event> {

        @Override
        public boolean filter(Event event) throws Exception {
            return event.userID.startsWith("J");
        }
    }
}
