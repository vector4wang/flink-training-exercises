package com.dataartisans.flinktraining.base.time;

/**
 * @author vector
 * @date: 2019/8/21 0021 17:31
 */
public class EventNumber {
    public String number;
    public long timeStamp;

    public EventNumber(String number, long timeStamp) {
        this.number = number;
        this.timeStamp = timeStamp;
    }
}
