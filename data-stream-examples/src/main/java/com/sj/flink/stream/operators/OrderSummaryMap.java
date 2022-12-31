package com.sj.flink.stream.operators;

import com.sj.flink.stream.pojo.OrderClientQty;
import com.sj.flink.stream.pojo.OrderItem;
import com.sj.flink.stream.pojo.OrderSkuQty;
import com.sj.flink.stream.pojo.OrderSummary;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.network.Client;

import java.util.Map;

public class OrderSummaryMap extends RichMapFunction<OrderItem, OrderItem> {

    transient MapState<String, Integer> skuQtyMap = null;
    transient MapState<String, Integer> clientQtyMap = null;

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        MapStateDescriptor<String,Integer> skuQtyMapStateDescriptor =
                new MapStateDescriptor<String, Integer>(
                        "skuQtyMapStateDescriptor",
                        TypeInformation.of(new TypeHint<String>(){}),
                        TypeInformation.of(new TypeHint<Integer>(){}));
        skuQtyMap = getRuntimeContext().getMapState(skuQtyMapStateDescriptor);

        MapStateDescriptor<String,Integer> clientQtyMapStateDescriptor =
                new MapStateDescriptor<String, Integer>(
                        "clientQtyMapStateDescriptor",
                        TypeInformation.of(new TypeHint<String>(){}),
                        TypeInformation.of(new TypeHint<Integer>(){}));
        clientQtyMap = getRuntimeContext().getMapState(clientQtyMapStateDescriptor);

    }

    @Override
    public OrderItem map(OrderItem orderItem) throws Exception {
        int skuQty = skuQtyMap.get(orderItem.getSkuNo()) == null? orderItem.getQuantity(): skuQtyMap.get(orderItem.getSkuNo()) + orderItem.getQuantity();
        skuQtyMap.put(orderItem.getSkuNo(), skuQty);
        int clientQty = clientQtyMap.get(orderItem.getClient()) == null? orderItem.getQuantity(): clientQtyMap.get(orderItem.getClient()) + orderItem.getQuantity();
        clientQtyMap.put(orderItem.getClient(), clientQty);

        /*
        OrderSkuQty orderSkuQty = new OrderSkuQty();
        orderSkuQty.setSkuNo(orderItem.getSkuNo());
        orderSkuQty.setQuantity(orderItem.getQuantity());

        OrderClientQty orderClientQty = new OrderClientQty();
        orderClientQty.setClient(orderItem.getClient());
        orderClientQty.setQuantity(orderItem.getQuantity());

        OrderSummary orderSummary = new OrderSummary();
        orderSummary.setOrderClientQty(orderClientQty);
        orderSummary.setOrderSkuQty(orderSkuQty);
         */

        /*
        StringBuffer sb = new StringBuffer();
        for(Map.Entry<String, Integer> entry: skuQtyMap.entries()){
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue()) ;
            System.out.println(sb.toString());
        }
         */
        System.out.println(orderItem.getSkuNo() + "=" + skuQtyMap.get(orderItem.getSkuNo()) + " " + orderItem.getClient() + "=" + clientQtyMap.get(orderItem.getClient()));
        orderItem.setQuantity(orderItem.getQuantity());
        return orderItem;
    }
}
