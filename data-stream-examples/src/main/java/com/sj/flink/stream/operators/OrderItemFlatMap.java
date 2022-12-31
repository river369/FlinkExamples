package com.sj.flink.stream.operators;

import com.sj.flink.stream.pojo.OrderItem;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class OrderItemFlatMap implements FlatMapFunction<String, OrderItem> {

    @Override
    public void flatMap(String s, Collector<OrderItem> collector) throws Exception {
        collector.collect(new OrderItem(s));
    }
}