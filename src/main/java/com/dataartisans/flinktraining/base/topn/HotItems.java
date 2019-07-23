package com.dataartisans.flinktraining.base.topn;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;

/**
 * @author vector
 * @date: 2019/7/23 0023 9:46
 *
 * 代码来自：http://wuchong.me/blog/2018/11/07/use-flink-calculate-hot-items/
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
        env.setParallelism(1);

        // TODO 需要准备数据
        URL resource = HotItems.class.getClassLoader().getResource("UserBehavior.csv");

        Path filePath = Path.fromLocalFile(new File(resource.toURI()));

        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] filedOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};

        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, filedOrder);

//        env.readFile()

        DataStream<UserBehavior> dataStreamSource = env.createInput(csvInput, pojoType);

        // 第一件是告诉 Flink 我们现在按照 EventTime 模式进行处理，Flink 默认使用 ProcessingTime 处理，所以我们要显式设置下。
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * 第二件事情是指定如何获得业务时间，以及生成 Watermark。org.apache.flink.streaming.api.watermark.Watermark 是用来追踪业务事件的概念，
         * 可以理解成 EventTime 世界中的时钟，用来指示当前处理到什么时刻的数据了。由于我们的数据源的数据已经经过整理，
         * 没有乱序，即事件的时间戳是单调递增的，所以可以将每条数据的业务时间就当做 Watermark。这里我们用 AscendingTimestampExtractor
         * 来实现时间戳的抽取和 Watermark 的生成。
         */
        DataStream<UserBehavior> timedData = dataStreamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                // 原始数据单位秒，将其转成毫秒
                return userBehavior.timestamp * 1000;
            }
        });

        // 统计点击量
        DataStream<UserBehavior> pvData = timedData.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {

                return userBehavior.behavior.equals("pv");
            }
        });

        /**
         * 由于要每隔5分钟统计一次最近一小时每个商品的点击量，
         * 所以窗口大小是一小时，每隔5分钟滑动一次。
         * 即分别要统计 [09:00, 10:00), [09:05, 10:05), [09:10, 10:10)… 等窗口的商品点击量
         */
        DataStream<ItemViewCount> windowData = pvData.keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes( 10))
                .aggregate(new CountAgg(), new WindowResultFunction());

        DataStream<String> topItems = windowData.keyBy("windowEnd").process(new TopNHotItems(3));

        topItems.print();

        env.execute("Hot Items Job");


    }
}
