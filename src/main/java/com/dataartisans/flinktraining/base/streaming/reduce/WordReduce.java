package com.dataartisans.flinktraining.base.streaming.reduce;

import com.dataartisans.flinktraining.base.streaming.model.WordWithCount;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author vector
 * @date: 2019/8/1 0001 14:00
 */
public class WordReduce implements ReduceFunction<WordWithCount> {

    @Override
    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
        return new WordWithCount(a.word, a.count + b.count);
    }
}
