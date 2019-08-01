package com.dataartisans.flinktraining.base.streaming.model;

/**
 * @author vector
 * @date 2019年8月1日14:01:22
 */
public class WordWithCount {
    public String word;
    public long count;

    public WordWithCount() {
    }

    public WordWithCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordWithCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}