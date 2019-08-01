package com.dataartisans.flinktraining.base.streaming.map;

import com.dataartisans.flinktraining.base.streaming.model.WordWithCount;
import com.dataartisans.flinktraining.base.streaming.state.WordCountState;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author vector
 * @date: 2019/8/1 0001 14:43
 */
public class WordCountFunction extends RichFlatMapFunction<WordWithCount,WordWithCount> {

    private transient ValueState<WordCountState> countState;

    /**
     * 初始化ValueState(未指定默认值，官方推荐自己处理)
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<WordCountState> descriptor = new ValueStateDescriptor<>("countState", WordCountState.class);
        countState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(WordWithCount in, Collector<WordWithCount> out) throws Exception {
        WordCountState lastState = countState.value();
        // 如第一次进更新state
        if (lastState == null) {
            lastState = new WordCountState(in.word, in.count);
            out.collect(in);
        }else{
            lastState.count = lastState.count + in.count;
            out.collect(new WordWithCount(in.word, lastState.count));
        }

        countState.update(lastState);
    }
}
