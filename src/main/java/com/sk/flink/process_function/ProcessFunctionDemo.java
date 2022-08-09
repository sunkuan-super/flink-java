package com.sk.flink.process_function;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description: 是一个富函数
 * @date: 2022/8/8 - 20:36
 */
public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> source = env.addSource(new ClickSourceDemo());

        SingleOutputStreamOperator<Event> watermarkDataStream =
                source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                }));

        SingleOutputStreamOperator<String> result = watermarkDataStream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                if (value.userID.equals("Mary")) {
                    out.collect(value.userID + " clicks " + value.urls);
                } else if (value.equals("Bob")) {
                    out.collect(value.userID);
                    out.collect(value.userID);
                }

                out.collect(value.toString());
                // ctx.timestamp() 打印的是事件时间
                System.out.println("ctx.timestamp() = " + ctx.timestamp());
                //ctx.output(new OutputTag<Event>("id"){});

                // Setting timers is only supported on a keyed streams.
                // ctx.timerService().registerEventTimeTimer(50000);
                System.out.println("子任务号是：" + getRuntimeContext().getIndexOfThisSubtask());
            }
        });

        result.print();

        env.execute();
    }
}
