package com.sk.flink.watermark;


import com.sk.flink.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/30 - 16:13
 */
public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> eventDataStreamSource = env.fromElements(
                new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 5000),
                new Event("James", "prod?id=10", 4000),
                new Event("Bob", "./fav", 3000),
                new Event("John", "./home", 4000),
                new Event("John", "./cart", 3000)
        );

        // assignTimestampsAndWatermarks属于DataStream的一个方法,所有实现了DataStream接口的类的对象都可以使用
        // 传入的参数为WatermarkStrategy<T>对象,WatermarkStrategy接口中有两个静态的方法，第一个forMonotonousTimestamps用作有序数据

//        eventDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
//                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//
//                    @Override
//                    public long extractTimestamp(Event event, long l) {
//                        return event.timestamp;
//                    }
//                }));

        // 第二个 forBoundedOutOfOrderness 用作乱序数据
        SingleOutputStreamOperator<Event> addWatermarkResult = eventDataStreamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new TimestampAssignerSupplier.SupplierFromSerializableTimestampAssigner<Event>(
                                new SerializableTimestampAssigner<Event>() {

                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                }
                        )));
        System.out.println(addWatermarkResult.getParallelism());

        addWatermarkResult.print().setParallelism(1);

        env.execute();

    }
}
