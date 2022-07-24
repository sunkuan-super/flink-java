package com.sk.flink.transformation;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/24 - 15:50
 */
public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 3000),
                new Event("James", "prod?id=10", 4000)
        );

        eventDataStreamSource.map(new MyMapRichFunction()).setParallelism(2).
                print().setParallelism(2);

        env.execute();
    }

    public static class MyMapRichFunction extends RichMapFunction<Event, Integer> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open()生命周期被调用：" + this.getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close()生命周期被调用：" + this.getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.userID.length();
        }
    }
}
