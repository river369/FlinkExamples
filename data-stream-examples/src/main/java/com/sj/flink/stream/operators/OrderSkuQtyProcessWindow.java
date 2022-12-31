package com.sj.flink.stream.operators;

import com.sj.flink.stream.pojo.OrderItem;
import com.sj.flink.stream.pojo.OrderSkuQty;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * IN, OUT, KEY, W extends Window
 */
public class OrderSkuQtyProcessWindow extends ProcessWindowFunction<OrderItem, OrderSkuQty, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<OrderItem> iterable, Collector<OrderSkuQty> collector) throws Exception {
        System.out.println(context.window().getStart() + "-------------" + context.window().getEnd());
        OrderSkuQty orderSkuQty = new OrderSkuQty();
        orderSkuQty.setSkuNo(s);
        orderSkuQty.setQuantity(0);
        for (OrderItem it: iterable) {
            orderSkuQty.setQuantity(orderSkuQty.getQuantity() + it.getQuantity());
        }
        collector.collect(orderSkuQty);
    }
}