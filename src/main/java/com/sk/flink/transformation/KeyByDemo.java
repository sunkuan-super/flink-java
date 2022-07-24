package com.sk.flink.transformation;

import com.sk.flink.bean.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/24 - 14:41
 */
public class KeyByDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 3000),
                new Event("James", "prod?id=10", 4000),
                new Event("Bob", "./fav", 3000),
                new Event("John", "./home", 4000),
                new Event("John", "./cart", 5000)
        );

        KeyedStream<Event, String> eventStringKeyedStream = eventDataStreamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.getUserID();
            }
        });

        KeyedStream<Event, String> eventStringKeyedStream1 = eventDataStreamSource.keyBy(event -> event.userID);

        // max与maxBy区别：max只更新选中的字段，其他字段还沿用之前的，maxBy直接更新所有字段
        SingleOutputStreamOperator<Event> result = eventStringKeyedStream.max("timestamp");
        SingleOutputStreamOperator<Event> result2 = eventStringKeyedStream.maxBy("timestamp");
        result.print("max");
        result2.print("maxBy");
        env.execute();
    }
}
