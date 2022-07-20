package com.sk.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/20 - 22:42
 */
public class ElementSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> integerDataStreamSource = environment.fromElements(1, 2, 3, 4).setParallelism(1);
        integerDataStreamSource.print();

        environment.execute();
    }
}
