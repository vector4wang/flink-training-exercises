## 状态
假定一个 source stream 中的事件消息都符合 e = {event_id:int, event_value:int} 这种模式。如果我们的目标是提取出每一条消息中的 event_value， 那么一个很简单的 map 操作就可以完成，这个 map 操作就是无状态的。但是，如果我们只想要输出那些 event_value 值比之前处理过的值都要高的那些消息，我们要如何实现呢？显然，我们需要记住已经处理过的消息中最大的 event_value，这就是有状态的操作

- 有状态
- 无状态



## 样本
我们的出租车数据集包含有关纽约市各个出租车的信息。每次乘车由两个事件代表：旅行开始和旅行结束事件。每个活动由11个领域组成：
```text
rideId         : Long      // a unique id for each ride
taxiId         : Long      // a unique id for each taxi
driverId       : Long      // a unique id for each driver
isStart        : Boolean   // TRUE for ride start events, FALSE for ride end events
startTime      : DateTime  // the start time of a ride
endTime        : DateTime  // the end time of a ride,
                           //   "1970-01-01 00:00:00" for start events
startLon       : Float     // the longitude of the ride start location
startLat       : Float     // the latitude of the ride start location
endLon         : Float     // the longitude of the ride end location
endLat         : Float     // the latitude of the ride end location
passengerCnt   : Short     // number of passengers on the ride
```

还有一个包含出租车费用数据的相关数据集，包括以下字段：

```text
rideId         : Long      // a unique id for each ride
taxiId         : Long      // a unique id for each taxi
driverId       : Long      // a unique id for each driver
startTime      : DateTime  // the start time of a ride
paymentType    : String    // CSH or CRD
tip            : Float     // tip for this ride
tolls          : Float     // tolls for this ride
totalFare      : Float     // total fare collected
```


## 学习地址

https://www.cnblogs.com/bjwu/p/9973521.html

https://training.ververica.com/


