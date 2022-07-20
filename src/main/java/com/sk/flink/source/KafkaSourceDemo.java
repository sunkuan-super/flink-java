package com.sk.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/20 - 22:36
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node90:9092");
        properties.setProperty("group.id", "consumer-group");
        // 有了下边的new SimpleStringSchema(),key.deserializer和value.deserializer可以不写
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaStream = environment.addSource(
                new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));
        kafkaStream.print();

        environment.execute();
    }
}
