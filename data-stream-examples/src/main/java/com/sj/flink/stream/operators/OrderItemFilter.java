package com.sj.flink.stream.operators;

import com.sj.flink.stream.pojo.OrderItem;
import org.apache.flink.api.common.functions.FilterFunction;

public class OrderItemFilter implements FilterFunction<OrderItem> {

    @Override
    public boolean filter(OrderItem orderItem) throws Exception {
        return orderItem.getQuantity() > 0;
    }
}