package com.dataartisans.flinktraining.base.streaming.task;

import com.dataartisans.flinktraining.base.streaming.map.WordCountFunction;
import com.dataartisans.flinktraining.base.streaming.map.WordTransform;
import com.dataartisans.flinktraining.base.streaming.model.WordWithCount;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 有状态的任务，如计算启动后所有的单词总数
 * @author vector
 * @date: 2019/8/1 0001 14:38
 */
public class SocketWindowCountStateful {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);

        DataStream<String> words = env.socketTextStream("192.168.1.33", 9091);

        DataStream<WordWithCount> result = words.flatMap(new WordTransform())
                .keyBy("word")
                .flatMap(new WordCountFunction());

        result.print();

        env.execute("Socket Count Word With State");
    }
}
