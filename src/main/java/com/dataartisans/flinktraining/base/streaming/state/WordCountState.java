package com.dataartisans.flinktraining.base.streaming.state;

import java.io.Serializable;

/**
 * @author vector
 * @date: 2019/8/1 0001 14:44
 */
public class WordCountState implements Serializable {
    public String word;
    public long count;

    public WordCountState(String word, long count) {
        this.word = word;
        this.count = count;
    }
}
