# Flink流式计算之WaterMark的使用

### 简介
可以转到官网查看详细的解释
[Event Time and Watermarks](https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/event_time.html#Event%20Time%20and%20Watermarks)

[Generating Timestamps / Watermarks](https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/event_timestamps_watermarks.html)

为了处理事件时间，Flink需要知道事件的时间戳，这意味着流中的每个数据元都需要分配其事件时间戳。这通常通过从数据元中的某个字段访问/提取时间戳来完成。

时间戳分配与生成水印密切相关，水印告诉系统事件时间的进展。

如事件A的水印为(2019-08-21 21:37:18(1566394638000))意思是"timestamp小于1566394638000的数据，都已经到达了。" 嗯，有点难理解，相信你看完下文就会有一个新的认识了

**注意:自1970-01-01T00：00：00Z的Java纪元以来，时间戳和水印都指定为毫秒。** 

### 作用
对于事件的发送和接收，理论上来说是按照顺序进行的，但是在真实的使用场景中，一定会出现很多乱序的消息，比如12:00产生的事件(日志)，由于网络和其他原因，导致flink在12:03分才接收到。
然而在窗口计算的时候，这类"迟到"的事件是不会被计算的，所以，为了解决此问题，就需要使用watermark来协助解决！
所以说：watermark是用于处理乱序事件的，而正确的处理乱序事件，通常用watermark机制结合window来实现。

### 如何分配
查看对应的Java api会发现有两种方法
```java
// 周期性水印
AssignerWithPeriodicWatermarks 

// 间断性水印
AssignerWithPunctuatedWatermarks
```
简单的理解就是周期性水印，一顿时间内即使未收到任何事件或元素时也需要发出水印，下面的案例我们就是用此方法

具体的方法为：
```java
xxxx.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<EventNumber>() {

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        // 通过实例化Watermark返回水印
        return null;
    }

    @Override
    public long extractTimestamp(EventNumber eventNumber, long previousElementTimestamp) {
    	// 从事件中抽取水印并返回
        return 0;
    }
});
}

```

一般来说，使用水印的时候都会指定一个"偏移量"，即抽取的时间戳减去偏移量即可认为是当前的水印

### 试验
我们使用socket来接收一些事件，代码如下
```java
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
```
还有一些示例数据
```text
1461756862000 	2016-04-27 19:34:22
1461756863000 	2016-04-27 19:34:23
1461756864000 	2016-04-27 19:34:24
1461756865000 	2016-04-27 19:34:25
1461756866000 	2016-04-27 19:34:26
1461756867000 	2016-04-27 19:34:27
1461756868000 	2016-04-27 19:34:28
1461756869000 	2016-04-27 19:34:29
1461756870000 	2016-04-27 19:34:30
1461756871000 	2016-04-27 19:34:31
1461756872000 	2016-04-27 19:34:32
1461756873000 	2016-04-27 19:34:33
1461756874000 	2016-04-27 19:34:34
```
发送数据的格式为"a,1461756862000"

