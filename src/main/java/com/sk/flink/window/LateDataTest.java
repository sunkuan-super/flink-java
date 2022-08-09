package com.sk.flink.window;

import com.sk.flink.bean.Event;
import com.sk.flink.bean.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/7 - 21:01
 */
public class LateDataTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 如果设置并行度大于1时，则窗口触发以多个并行子任务中的最小的watermark为准向下传递
        env.setParallelism(1);
        // 设置生成Watermark的周期性
        env.getConfig().setAutoWatermarkInterval(100);
        // 获取源
        SingleOutputStreamOperator<Event> source = env.socketTextStream("hadoop1", 7077).map(
                new MapFunction<String, Event>() {
                    @Override
                    public Event map(String s) throws Exception {
                        String[] words = s.split(",");
                        return new Event(words[0].trim(), words[1].trim(), Long.parseLong(words[2].trim()));
                    }
                }
        );

        SingleOutputStreamOperator<Event> watermarkSource =
                source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).
                        withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));


        watermarkSource.print("data");

        System.out.println(watermarkSource.getParallelism());
        // 注意： new OutputTag<Event>("outputTag")后需要跟{},不然会类型擦除
        OutputTag<Event> outputTag = new OutputTag<Event>("outputTag") {
        };
        // WindowFunction
        SingleOutputStreamOperator<UrlViewCount> result = watermarkSource
                .keyBy(event -> event.urls)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 设置watermark后再延迟时间,比如watermark到10的时候,实际的数据可能到了12, 实际该关0-10的窗口,但是又延迟了1分钟,所以需要watermark到了70的时候才去关闭窗口
                .allowedLateness(Time.minutes(1))
                // 窗口已经关闭后,又有延迟数据到来,这时将延迟数据放入窗口侧输出流,放入这个地方的数据,不参与窗口的计算,只能通过最后人为去判断去更新
                .sideOutputLateData(outputTag)
                // 针对第一个参数(增量聚合函数)来处理窗口数据,每来一个数据就做一次聚合。这里的全窗口函数就不再缓存所有数据了
                // 等到窗口需要触发计算时, 则调用第二个参数(全窗口函数)的处理逻辑输出结果，第一个参数的输出作为第二个参数的输入
                .aggregate(new UrlViewCountExample.UrlViewCountAgg(), new UrlViewCountExample.UrlViewCountResult());

        result.print();

        DataStream<Event> sideOutput = result.getSideOutput(outputTag);
        sideOutput.print("late");

        env.execute();

    }
}
