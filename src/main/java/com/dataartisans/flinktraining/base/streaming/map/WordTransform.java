package com.dataartisans.flinktraining.base.streaming.map;

import com.dataartisans.flinktraining.base.streaming.model.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author vector
 * @date: 2019/8/1 0001 13:55
 */
public class WordTransform implements FlatMapFunction<String, WordWithCount> {

    @Override
    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
        for (String s1 : value.split("\\s")) {
            out.collect(new WordWithCount(s1, 1L));
        }
    }
}
