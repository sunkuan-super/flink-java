package com.sk.flink.state;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/24 - 23:25
 */
public class OperatorStateBufferingSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new ClickSourceDemo())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long recordTimestamp) {
                                return event.timestamp;
                            }
                        }));

        streamOperator.print("input");

        streamOperator.addSink(new BufferingSink(10));

        env.execute();
    }

    // 自定义实现SinkFunction
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {

        // 定义当前类的属性，批量
        private final int threshold;

        public BufferingSink(int threshold){
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        private List<Event> bufferedElements;

        // 定义一个算子状态
        private ListState<Event> checkpointedState;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 清空状态,保持与bufferedElements中的数据保持一致
            checkpointedState.clear();

            // 对状态进行持久化，复制缓存的列表到列表状态
            for (Event event : bufferedElements) {
                checkpointedState.add(event);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-element", Event.class);
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if(context.isRestored()){

                // 如果从故障中恢复出来的，需要将ListState中的所有元素复制到列表中
                // 从故障中恢复出来的数据，会轮询到各个分区中
                for (Event event : checkpointedState.get()) {
                    bufferedElements.add(event);
                }
            }
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);  // 缓存到列表
            // 判断如果达到阈值，就批量写入
            if(bufferedElements.size() == threshold){
                // 用打印到控制台模拟写入外部系统
                for (Event element : bufferedElements) {
                    System.out.println(element);
                }

                // 输出完毕，
                bufferedElements.clear();
            }
        }
    }
}
