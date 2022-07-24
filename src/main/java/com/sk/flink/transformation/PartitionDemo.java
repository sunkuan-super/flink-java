package com.sk.flink.transformation;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/24 - 17:08
 */
public class PartitionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(
                new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 3000),
                new Event("James", "prod?id=10", 4000),
                new Event("Bob", "./fav", 3000),
                new Event("John", "./home", 4000),
                new Event("Bob", "./fav", 4000),
                new Event("John", "./cart", 5000)
        );

        // 随机分发的方式
        // eventDataStreamSource.shuffle().print().setParallelism(4);

        // 轮询的方式 上游的数据轮询的方式往下游的每一个分区里放，或者并行子任务里放，往下游的全部分区都要放
        //eventDataStreamSource.rebalance().print().setParallelism(4);

        // 重缩放分区 rescale() 上游必须是多个分区，下游也多个分区，上游的某一个分区只对应下游的部分分区
        DataStreamSource<Integer> integerDataStreamSource = env.addSource(new MyParallelSourceFunction());
        // integerDataStreamSource.rescale().print().setParallelism(4);

        // 广播的方式 上游的一个并行子任务中的数据，相同的数据会往下游的并行子任务中都传一个
        // eventDataStreamSource.broadcast().print().setParallelism(4);

        // 全局分区 全局分区不管后边设置多少并行度 并行度都为1
        // eventDataStreamSource.global().print().setParallelism(4);

        // 自定义重分区 自定义分几个区定了后，再在后边设置并行度不生效
        integerDataStreamSource.partitionCustom(new Partitioner<Integer>() {
            // key, numberOfChannels
            @Override
            public int partition(Integer key, int numberOfChannels) {
                return key % 2;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer integer) throws Exception {
                return integer;
            }
        }).print().setParallelism(4);

        env.execute();
    }

    public static class MyParallelSourceFunction extends RichParallelSourceFunction<Integer> {

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            for (int i = 1; i <= 8; i++) {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                if (indexOfThisSubtask % 2 == 0) {
                    ctx.collect(i);
                }
            }
        }

        @Override
        public void cancel() {

        }
    }
}
