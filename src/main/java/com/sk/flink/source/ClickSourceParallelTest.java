package com.sk.flink.source;

import com.sk.flink.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/21 - 22:44
 */
public class ClickSourceParallelTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(10);

        DataStreamSource<Event> customStream = environment.addSource(new ClickSourceParallelDemo()).setParallelism(5);
        customStream.print();
        environment.execute();
    }
}
