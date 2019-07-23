package com.dataartisans.flinktraining.base.topn;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author vector
 * @date: 2019/7/23 0023 10:53
 */
public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long acc) {
        return acc + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
