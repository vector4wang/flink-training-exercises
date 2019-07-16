package com.dataartisans.flinktraining.base.join;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author vector
 * @date: 2019/7/16 0016 11:20
 */
public class ConnectedStreamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = environment.fromElements("DROP", "IGNORE").keyBy(item -> item);
        DataStream<String> streamOfWords = environment.fromElements("data", "DROP", "artisans", "IGNORE").keyBy(item -> item);

        control.connect(streamOfWords).flatMap(new ControlFunction()).print();

        environment.execute("Connected Stream Example");
    }


    private static class ControlFunction extends RichCoFlatMapFunction<String,String,String> {

        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration parameters) throws Exception {
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("blocked", Boolean.class));
        }

        @Override
        public void flatMap1(String s, Collector<String> collector) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String s, Collector<String> collector) throws Exception {
            if (blocked.value() == null) {
                collector.collect(s);
            }
        }
    }
}
