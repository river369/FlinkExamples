package com.sj.flink.stream.operators;

import com.sj.flink.stream.pojo.OrderSkuQty;
import org.apache.flink.api.common.functions.ReduceFunction;

public class OrderSkuQtyReduce implements ReduceFunction<OrderSkuQty> {

    @Override
    public OrderSkuQty reduce(OrderSkuQty orderSkuQty, OrderSkuQty t1) throws Exception {
        orderSkuQty.setQuantity(orderSkuQty.getQuantity() + t1.getQuantity());
        return orderSkuQty;
    }
}