package com.dataartisans.flinktraining.base.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author vector
 * @date: 2019/8/2 0002 9:32
 */
public class MapStreamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> streamOfWords = environment.fromElements("data", "DROP", "artisans", "IGNORE");
        streamOfWords.map(new MapFunction<String, String>() {
            /**
             * MapFunction<T, O>   T: stream中的元素；O: 输出返回的类型
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public String map(String value) throws Exception {
                return value.toLowerCase();
            }
        }).print();

        environment.execute("MapStreamExample");
    }
}
