package com.sk.flink.sink;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/24 - 18:34
 */
public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

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

        Path path = new Path("./output");
        new SimpleStringEncoder<>("UTF-8");
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(path, new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder().
                                withMaxPartSize(1024 * 1024 * 1024).
                                withRolloverInterval(TimeUnit.MINUTES.toMillis(15)).
                                withInactivityInterval(TimeUnit.MINUTES.toMillis(5)).
                                build()
                ).build();


        eventDataStreamSource.map(event -> event.toString()).addSink(streamingFileSink);

        env.execute();
    }
}
