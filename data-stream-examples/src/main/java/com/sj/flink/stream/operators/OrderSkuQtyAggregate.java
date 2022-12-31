package com.sj.flink.stream.operators;

import com.sj.flink.stream.pojo.OrderItem;
import com.sj.flink.stream.pojo.OrderSkuQty;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * AggregateFunction<IN, ACC, OUT>
 */
public class OrderSkuQtyAggregate implements AggregateFunction<OrderItem, OrderSkuQty, OrderSkuQty> {

    @Override
    public OrderSkuQty createAccumulator() {
        return new OrderSkuQty();
    }

    @Override
    /**
     * ACC add(IN var1, ACC var2);
     * input as a orderItem, add the value to ACC orderSkuQty
     */
    public OrderSkuQty add(OrderItem orderItem, OrderSkuQty orderSkuQty) {
        System.out.println("add " + orderItem + "  " + orderSkuQty);
        orderSkuQty.setSkuNo(orderItem.getSkuNo());
        orderSkuQty.setQuantity(orderSkuQty.getQuantity() + orderItem.getQuantity());
        return orderSkuQty;
    }

    @Override
    /**
     * OUT getResult(ACC var1);
     */
    public OrderSkuQty getResult(OrderSkuQty orderSkuQty) {
        return orderSkuQty;
    }

    @Override
    /**
     * ACC merge(ACC var1, ACC var2);
     */
    public OrderSkuQty merge(OrderSkuQty orderSkuQty, OrderSkuQty acc1) {
        System.out.println("merge " + orderSkuQty + "  " + acc1);
        orderSkuQty.setQuantity(orderSkuQty.getQuantity() + acc1.getQuantity());
        return orderSkuQty;
    }
}