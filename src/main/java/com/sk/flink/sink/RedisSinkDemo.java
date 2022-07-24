package com.sk.flink.sink;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/24 - 20:29
 */
public class RedisSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> dataStreamSource = env.addSource(new ClickSourceDemo());

        // 创建一个jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().
                setHost("localhost").
                build();

        // 写入redis
        dataStreamSource.addSink(new RedisSink<>(config, new MyRedisMapper()));
        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<Event>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }

        @Override
        public String getKeyFromData(Event event) {
            return event.userID;
        }

        @Override
        public String getValueFromData(Event event) {
            return event.urls;
        }
    }
}
