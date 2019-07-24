package com.dataartisans.flinktraining.base.topn;

/**
 * @author vector
 * @date: 2019/7/23 0023 11:27
 */
public class ItemViewCount {
    public long itemId;     // 商品ID
    public long windowStart; // 窗口开始时间戳
    public long windowEnd;  // 窗口结束时间戳
    public long viewCount;  // 商品的点击量

    public static ItemViewCount of(long itemId, long windowStart, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.itemId = itemId;
        result.windowStart = windowStart;
        result.windowEnd = windowEnd;
        result.viewCount = viewCount;
        return result;
    }
}
