package com.sj.flink.stream.operators;

import com.sj.flink.stream.pojo.OrderItem;
import org.apache.flink.api.common.functions.MapFunction;

public class OrderItemMap implements MapFunction<String, OrderItem> {

    @Override
    public OrderItem map(String s) throws Exception {
        return new OrderItem(s);
    }
}