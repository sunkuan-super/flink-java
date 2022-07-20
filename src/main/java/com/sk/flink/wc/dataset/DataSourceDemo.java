package com.sk.flink.wc.dataset;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/12 - 21:22
 */
@Slf4j
public class DataSourceDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDataSource.
                flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将一行文本进行分析
            String[] words = line.split(" ");
            // 将每个单词转换成二元组输出
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOne.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);
        log.info("--------------------------------------");
        sum.print();
    }

}
