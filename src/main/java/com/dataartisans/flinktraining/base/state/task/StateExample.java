package com.dataartisans.flinktraining.base.state.task;

import com.dataartisans.flinktraining.base.topn.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

/**
 * @author vector
 * @date: 2019/8/6 0006 14:27
 */
public class StateExample {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        CsvReader csvReader = env.readCsvFile("D:\\data\\flink\\trainingData\\UserBehavior.csv");


        DataSource<UserBehavior> userBehaviorDataSource = csvReader.pojoType(UserBehavior.class,"userId", "itemId", "categoryId", "behavior", "timestamp");

       userBehaviorDataSource.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.behavior.equals("fav");
            }
        }).groupBy("itemId").reduceGroup(new GroupReduceFunction<UserBehavior, String>() {
            @Override
            public void reduce(Iterable<UserBehavior> values, Collector<String> out) throws Exception {
                Long total = 0L;
                Long item = 0L;
                for (UserBehavior value : values) {
                    total++;
                    item = value.itemId;
                }

                out.collect(item + " 被收藏了 " +total + "次");
            }
        }).print();

//        env.execute("StateExample!");
    }
}
