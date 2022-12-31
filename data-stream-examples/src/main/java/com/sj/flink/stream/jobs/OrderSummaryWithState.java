package com.sj.flink.stream.jobs;

import com.sj.flink.stream.operators.OrderItemFilter;
import com.sj.flink.stream.operators.OrderItemMap;
import com.sj.flink.stream.operators.OrderSkuQtyAggregate;
import com.sj.flink.stream.operators.OrderSummaryMap;
import com.sj.flink.stream.pojo.OrderItem;
import com.sj.flink.stream.pojo.OrderSkuQty;
import com.sj.flink.stream.pojo.OrderSummary;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class OrderSummaryWithState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<OrderItem> orderItemDataStream = env.fromElements(
                "B001 Jack 1671537515000 3",
                "B002 Jack 1671537515000 2",
                "B001 Tom 1671537518000 1",
                "B001 Tom 1671537518000 1",
                "B003 Jason 1671537519000 5",
                "B002 Tom 1671537519000 7",
                "B005 Jason 1671537519000 -1",
                "B001 Green 1671537539000 4"
        ).map(new OrderItemMap())
                .filter(new OrderItemFilter());
        orderItemDataStream.print();

        DataStream<OrderItem> orderSkuQtyDataStream = orderItemDataStream
                .keyBy(OrderItem::getSkuNo) // The state value can be seen in each partition
                .map(new OrderSummaryMap());
        orderSkuQtyDataStream.print();

        env.execute("Order Summary With State");
    }
}