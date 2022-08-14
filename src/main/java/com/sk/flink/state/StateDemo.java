package com.sk.flink.state;

import com.sk.flink.bean.Event;
import com.sk.flink.source.ClickSourceDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/14 - 19:41
 */
public class StateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSourceDemo())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.keyBy(event -> event.userID)
                .flatMap(new MyFlatMap()).print();

        env.execute();
    }

    // 实现自定义的FlatMapFunction,用于KeyedState测试
    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {

        // 定义值状态
        ValueState<Event> myValueState;

        // 列表状态
        ListState<Event> listState;

        // 映射状态
        MapState<String, Long> mapState;

        // 定义ReducingState
        ReducingState<Event> reducingState;

        // 定义AggregatingState
        AggregatingState<Event, String> aggregatingState;

        // 增加一个本地变量进行对比
        Long count;


        @Override
        public void open(Configuration parameters) throws Exception {

            count = 0L;

            myValueState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("valueState", Event.class));

            // public ListStateDescriptor(String name, Class<T> elementTypeClass)
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("listState", Event.class));

            // public MapStateDescriptor(String name, Class<UK> keyClass, Class<UV> valueClass)
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("mapState", String.class, Long.class));

            // String name, ReduceFunction<T> reduceFunction, Class<T> typeClass
            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("reducingState",
                    new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event event, Event t1) throws Exception {
                            return new Event(event.userID, event.urls, t1.timestamp);
                        }
                    },
                    Event.class
            ));

            // public AggregatingStateDescriptor(String name, AggregateFunction<IN, ACC, OUT> aggFunction, Class<ACC> stateType)
            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("aggregatingState",
                    new AggregateFunction<Event, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Event event, Long accumulator) {
                            return accumulator + 1;
                        }

                        @Override
                        public String getResult(Long accumulator) {
                            return "count: " + accumulator;
                        }

                        @Override
                        public Long merge(Long aLong, Long acc1) {
                            return aLong + acc1;
                        }
                    },
                    Long.class));
        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            // 此方法中的所有状态都是针对当前的key, 每个key的状态都是独立的
            System.out.println(myValueState.value());
            myValueState.update(event);
            System.out.println("myValue: " + myValueState.value());

            listState.add(event);
            System.out.println(listState.get());

            mapState.put(event.userID, mapState.get(event.userID) == null ? 1 : mapState.get(event.getUserID()) + 1);
            System.out.println("my map value: " + event.userID + " -> " + mapState.get(event.userID));

            reducingState.add(event);
            System.out.println("reduce state: " + reducingState.get());

            aggregatingState.add(event);
            System.out.println("aggregate state: " + aggregatingState.get());

            count++;

            System.out.println("count=" + count);
        }
    }
}
