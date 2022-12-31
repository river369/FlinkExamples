package com.sj.flink.stream.operators;

import com.sj.flink.stream.pojo.OrderItem;
import com.sj.flink.stream.pojo.OrderSkuQty;
import com.sun.tools.corba.se.idl.constExpr.Or;
import org.apache.flink.api.common.functions.MapFunction;

public class OrderItemToSkuQtyMap implements MapFunction<OrderItem, OrderSkuQty> {

    @Override
    public OrderSkuQty map(OrderItem orderItem ) throws Exception {
        OrderSkuQty orderSkuQty = new OrderSkuQty();
        orderSkuQty.setSkuNo(orderItem.getSkuNo());
        orderSkuQty.setQuantity(orderItem.getQuantity());
        return orderSkuQty;
    }
}