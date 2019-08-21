package com.dataartisans.flinktraining.base.time;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 *
 * @author vector
 * @date: 2019/8/21 0021 16:47
 */
public class WaterMarkSample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> dataStream = env.socketTextStream("192.168.1.95", 9999);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<EventNumber> formatStream = dataStream.map(line -> {
            String[] split = line.split(",");
            return new EventNumber(split[0], Long.parseLong(split[1]));
        });

        DataStream<EventNumber> watermarks = formatStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<EventNumber>() {

            long currentTime = 0L;

            long maxOffsetTime = 16000L; // 10s

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            Watermark a = null;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                a = new Watermark(currentTime - maxOffsetTime);
                return a;
            }

            @Override
            public long extractTimestamp(EventNumber eventNumber, long previousElementTimestamp) {
                currentTime = eventNumber.timeStamp;
                System.out.println("timestamp:" + eventNumber.number + "," + eventNumber.timeStamp + "|" + format.format(eventNumber.timeStamp) + "," + a.toString());
                return currentTime;
            }
        });

        DataStream<String> result = watermarks
                .keyBy((KeySelector<EventNumber, String>) value -> value.number)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new WindowFunction<EventNumber, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<EventNumber> input, Collector<String> out) throws Exception {
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        Iterator<EventNumber> iterator = input.iterator();

                        int count = 0;
                        while (iterator.hasNext()) {
                            iterator.next();
                            count++;
                        }
                        out.collect("(" + key + " , " + count + " , " + format.format(window.getStart()) + " ~ " + format.format(window.getEnd()) + ")");
                    }
                });

        result.print();
        env.execute();
    }
}
