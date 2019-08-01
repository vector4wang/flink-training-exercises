package com.dataartisans.flinktraining.base.streaming.task;

import com.dataartisans.flinktraining.base.streaming.map.WordTransform;
import com.dataartisans.flinktraining.base.streaming.model.WordWithCount;
import com.dataartisans.flinktraining.base.streaming.reduce.WordReduce;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 比较重要的就是如何打包
 *
 * @author vector
 * @date: 2019/7/9 0009 14:34
 */
public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        final String host;
        final int port;

        try {
            final ParameterTool parameterTool = ParameterTool.fromArgs(args);
            host = parameterTool.has("hostname") ? parameterTool.get("hostname") : "localhost";
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.out.println("No port specified! please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        // 1、创建flink的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2、指定数据源
        DataStream<String> text = env.socketTextStream(host, port);

        // 3、转换操作
        DataStream<WordWithCount> windowCounts = text
                .flatMap(new WordTransform())
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .reduce(new WordReduce());

        windowCounts.print().setParallelism(1);


        // 4、提交执行
        env.execute("Socket Window WordCount Reset!!!");

    }


}
