package com.sk.flink.stream;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description: 分流demo
 * @date: 2022/8/10 - 20:09
 */
public class SplitStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 输入源
        DataStreamSource<Event> originSource = env.addSource(new ClickSourceDemo());

        SingleOutputStreamOperator<Event> watermarkSource = originSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }));

        // 定义输出标签
        OutputTag<Tuple3<String, String, Long>> maryOutputTag = new OutputTag<Tuple3<String, String, Long>>("Mary"){};
        OutputTag<Tuple2<String, String>> bobOutputTag = new OutputTag<Tuple2<String, String>>("Bob"){};


        // 根据名称分成3个流
        // 主流与侧输出流类型可以不一样，侧输出流之间的类型也可以不一样
        SingleOutputStreamOperator<Event> mainStream = watermarkSource.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
                if ("Mary".equals(event.getUserID())) {
                    // 侧输出流用ctx.output输出
                    ctx.output(maryOutputTag, Tuple3.of(event.getUserID(), event.getUrls(), event.getTimestamp()));
                } else if ("Bob".equals(event.getUserID())) {
                    ctx.output(bobOutputTag, Tuple2.of(event.getUserID(), event.getUrls()));
                } else {
                    // 主流用out.collect输出
                    out.collect(event);
                }
            }
        });

        // 输出主流
        mainStream.print();

        // 取出侧输出流
        DataStream<Tuple3<String, String, Long>> maryStream = mainStream.getSideOutput(maryOutputTag);
        DataStream<Tuple2<String, String>> bobStream = mainStream.getSideOutput(bobOutputTag);

        maryStream.print("mary");
        bobStream.print("bob");

        env.execute();
    }
}
