package com.sk.flink.sink;

import com.sk.flink.bean.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/24 - 21:04
 */
public class ESSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("Mary", "./home", 1000),
                new Event("Bob", "./cart", 2000),
                new Event("John", "./fav", 3000),
                new Event("James", "prod?id=10", 4000)
        );

        // 定义host列表
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop1", 9200));

        // 定义esSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {

            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

                HashMap<String, String> map = new HashMap<>();
                map.put(event.userID, event.urls);

                IndexRequest request = Requests.indexRequest()
                        // 类似于表名
                        .index("clicks")
                        // es6需要定义type es7就不需要了
                        .type("type")
                        .source(map);

                requestIndexer.add(request);
            }
        };

        // 写入ES
        eventDataStreamSource.addSink(new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction).build());

        env.execute();
    }
}
