package com.sk.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/20 - 22:38
 */
public class CollectionSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        environment.setParallelism(1);

        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        DataStreamSource<Integer> integerDataStreamSource = environment.fromCollection(list).setParallelism(1);
        integerDataStreamSource.print();

        environment.execute();
    }
}
