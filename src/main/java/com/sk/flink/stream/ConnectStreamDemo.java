package com.sk.flink.stream;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/10 - 21:05
 */
public class ConnectStreamDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 读入两条流
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> stream2 = env.fromElements(4L, 5L, 6L);

        // 两条流连接, 谁调用的connect方法谁是流1
        ConnectedStreams<Long, Integer> connectStream = stream2.connect(stream1);
        // 实现CoMapFunction接口, stream2调用的connect方法, stream2是第一个流,对应下边的IN1, stream1是第二个流,对应IN2,合并的结果为OUT
        // CoMapFunction<IN1, IN2, OUT>
        connectStream.map(new CoMapFunction<Long, Integer, String>() {
            @Override
            public String map1(Long value) throws Exception {
                return "Long: " + value;
            }

            @Override
            public String map2(Integer value) throws Exception {
                return "Integer: " + value;
            }
        }).print("connectStream");

        env.execute();
    }
}
