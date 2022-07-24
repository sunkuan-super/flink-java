package com.sk.flink.sink;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/24 - 19:53
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop1:9092");
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<String> result = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] fields = s.split(",");

                return new Event(fields[0].trim(), fields[1].trim(), Long.parseLong(fields[2].trim())).toString();
            }
        });

        result.print();
        result.addSink(new FlinkKafkaProducer<String>("hadoop1:9092", "events", new SimpleStringSchema()));

        env.execute();
    }
}
