package com.sk.flink.stream;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/10 - 20:55
 */
public class UnionStreamDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 输入源1
        SingleOutputStreamOperator<Event> originSource1 = env.socketTextStream("hadoop1", 7777)
                .map(data -> {
                    String[] words = data.split(",");
                    return new Event(words[0].trim(), words[1].trim(), Long.valueOf(words[2].trim()));
                });

        // 输入源1设置水位线
        SingleOutputStreamOperator<Event> watermarkSource1 = originSource1.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }));

        watermarkSource1.print("source1");

        // 输入源2
        SingleOutputStreamOperator<Event> originSource2 = env.socketTextStream("hadoop1", 8888)
                .map(data -> {
                    String[] words = data.split(",");
                    return new Event(words[0].trim(), words[1].trim(), Long.valueOf(words[2].trim()));
                });

        // 输入源2设置水位线
        SingleOutputStreamOperator<Event> watermarkSource2 = originSource2.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }));

        watermarkSource2.print("source2");

        // 合流操作
        DataStream<Event> unionStream = watermarkSource1.union(watermarkSource2);

        unionStream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                // 合流的水位线以两条流最小的watermark为准
                out.collect("水位线：" + ctx.timerService().currentWatermark());
            }
        }).print();

        env.execute();
    }

}