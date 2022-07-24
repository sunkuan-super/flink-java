package com.sk.flink.source;

import com.sk.flink.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/21 - 22:44
 */
public class ClickSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // DataStreamSource<Event> customStream = environment.addSource(new ClickSourceDemo()).setParallelism(2);
        // 不能给source设置并行度大于2,不是并行流,会报错 The parallelism of non parallel operator must be 1
        DataStreamSource<Event> customStream = environment.addSource(new ClickSourceDemo());
        customStream.print();
        environment.execute();
    }
}
