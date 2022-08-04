package com.sk.flink.watermark;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.omg.CORBA.PRIVATE_MEMBER;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/30 - 17:41
 */
public class CustomWatermarkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 可以设置周期性生成Watermark的频率，默认是200ms，也可以自己设置
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<Event> source = env.fromElements(
                new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 5000),
                new Event("James", "prod?id=10", 4000),
                new Event("Bob", "./fav", 3000),
                new Event("John", "./home", 4000),
                new Event("John", "./cart", 3000)
        );

        source.assignTimestampsAndWatermarks(new CustomWatermarkStrategy());
    }


    private static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {

                @Override
                public long extractTimestamp(Event event, long l) {
                    return event.timestamp;
                }
            };
        }
    }

    /**
     * 周期性生成Watermark
     */
    private static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {
        private Long delayTime = 5000L; // 延迟时间
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳

        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            // 每来一条数据就调用一次
            maxTs = Math.max(event.timestamp, maxTs); // 更新最大时间戳
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            /**发射水位线，默认 200ms 调用一次
             * 我们在 onPeriodicEmit()里调用 output.emitWatermark()，就可以发出水位线了；这个方法 由系统框架周期性地调用，默认 200ms 一次
             * 但具体什么时候生
             * 成与数据无关
             */
            watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }

    /**
     * 自定义断点式水位线生成器Punctuated
     */
    private static class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            // 只有在遇到特定的 itemId 时，才发出水位线
            if (event.userID.equals("Mary")) {
                watermarkOutput.emitWatermark(new Watermark(event.timestamp - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 不需要做任何事情，因为我们在 onEvent 方法中发射了水位线
        }
    }
}
