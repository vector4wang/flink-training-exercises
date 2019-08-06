package com.dataartisans.flinktraining.base.join;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author vector
 * @date: 2019/8/2 0002 9:35
 */
public class FlatMapStreamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> streamOfWords = environment.fromElements("data", "DROP", "artisans", "IGNORE");

        streamOfWords.flatMap(new FlatMapFunction<String, String>() {
            /**
             * FlatMapFunction<T, O> T：stream中的元素；O: 需要返回的元素，0或1或n个
             *
             * 跟map区别： 是否有返回值  flatmap直接通过O来收集需要返回的值
             *
             * @param value
             * @param out
             * @throws Exception
             */
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                if (value.equals(value.toUpperCase())) {
                    out.collect(value);
                }
            }
        }).print();

        environment.execute("FlatMapStreamExample");
    }
}
