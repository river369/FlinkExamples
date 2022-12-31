package com.sj.flink.stream.operators;

import com.sj.flink.stream.pojo.OrderItem;
import com.sj.flink.stream.pojo.OrderSkuQty;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * please ignore it,  the apply is legacy way. WindowFunction (Legacy) #
 */
@Deprecated
public class OrderSkuQtyWindow implements WindowFunction<OrderItem, OrderSkuQty, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<OrderItem> iterable, Collector<OrderSkuQty> collector) throws Exception {
        OrderSkuQty orderSkuQty = new OrderSkuQty();
        orderSkuQty.setSkuNo(s);
        for (OrderItem t: iterable) {
            orderSkuQty.setQuantity(orderSkuQty.getQuantity() + t.getQuantity());
        }
        collector.collect(orderSkuQty);
    }
}