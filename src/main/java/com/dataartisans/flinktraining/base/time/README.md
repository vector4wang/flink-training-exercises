# Flink流式计算之WaterMark的使用

### 简介
可以转到官网查看详细的解释
[Event Time and Watermarks](https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/event_time.html#Event%20Time%20and%20Watermarks)

[Generating Timestamps / Watermarks](https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/event_timestamps_watermarks.html)

为了处理事件时间，Flink需要知道事件的时间戳，这意味着流中的每个数据元都需要分配其事件时间戳。这通常通过从数据元中的某个字段访问/提取时间戳来完成。

时间戳分配与生成水印密切相关，水印告诉系统事件时间的进展。

有两种方法可以分配时间戳并生成水印：
- 直接在数据流源中
- 通过时间戳分配器/水印生成器：在Flink中，时间戳分配器还定义要发出的水印

**注意:自1970-01-01T00：00：00Z的Java纪元以来，时间戳和水印都指定为毫秒。** 

### 作用
对于事件的发送和接受，理论上来说是按照顺序进行的，但是在真实的使用场景中，一定会出现很多乱序的消息，比如12:00产生的事件(日志)，由于网络和其他原因，导致flink在12:03分才接收到
在窗口计算的时候，这类"迟到"的事件是不会被正常计算的，所以，为了解决此问题，就需要使用watermark来协助解决！
所以说：watermark是用于处理乱序事件的，而正确的处理乱序事件，通常用watermark机制结合window来实现。

https://blog.csdn.net/lmalds/article/details/52704170
https://www.jianshu.com/p/94c3865666fa