package com.sk.flink.sink;

import com.sk.flink.bean.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/24 - 21:29
 */
public class MySQLSinkDemo {
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

        eventDataStreamSource.addSink(JdbcSink.sink(
                "insert into clicks (user, url) values (?, ?)",
                ((preparedStatement, event) -> {
                    preparedStatement.setString(1, event.userID);
                    preparedStatement.setString(2, event.urls);
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/sk?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=true")
                        .withDriverName("com.mysql.cj.jdbc.Driver").withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

        env.execute();

//        eventDataStreamSource.addSink(JdbcSink.sink(
//                "insert into clicks (user, url) values (?, ?)",
//                ((statement, event) -> {
//                    statement.setString(1, event);
//                    statement.setString(2, event.urls);
//                }),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("jdbc:mysql://localhost:3306/sk")
//                        .withDriverName("com.mysql.cj.jdbc.Driver").withUsername("root")
//                        .withPassword("123456")
//        ));

    }
}
