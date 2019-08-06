package com.dataartisans.flinktraining.base.join;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 合并两个stream
 * @author wangxc
 * @date: 2019-08-02 07:48
 *
 */
public class UnionStreamExample {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setParallelism(1);
		DataStream<String> control = environment.fromElements("DROP", "IGNORE").keyBy(item -> item);
		DataStream<String> streamOfWords = environment.fromElements("data", "DROP", "artisans", "IGNORE");

		control.union(streamOfWords).keyBy(item -> item).print();
		environment.execute("UnionStreamExample!");
	}
}
