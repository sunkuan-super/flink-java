package com.sk.flink.transformation;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/23 - 17:00
 */
public class MapDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 3000),
                new Event("James", "prod?id=10", 4000)
        );

        // 1、第一种方式通过自定义类实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = eventDataStreamSource.map(new MyMapFunction());

        // 2、使用匿名内部类
        SingleOutputStreamOperator<String> result2 = eventDataStreamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.userID;
            }
        });

        // 3、使用Lambda表达式,使用Lambda表达式可能需要调用return方法，手动指定返回类型，因为Java的类型擦除
        SingleOutputStreamOperator<String> result3 = eventDataStreamSource.map(event -> event.userID);

        // result1.print();
        // result2.print();
        result3.print();
        env.execute();
    }

    // 自定义MapFunction
    // 泛型中Event代表输入类型 String代表输出类型
    public static class MyMapFunction implements MapFunction<Event, String>{

        @Override
        public String map(Event event) throws Exception {
            return event.userID;
        }
    }
}
