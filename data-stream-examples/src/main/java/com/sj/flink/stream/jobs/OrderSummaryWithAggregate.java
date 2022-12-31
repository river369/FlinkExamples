package com.sj.flink.stream.jobs;

import com.sj.flink.stream.operators.*;
import com.sj.flink.stream.operators.OrderItemMap;
import com.sj.flink.stream.pojo.OrderItem;
import com.sj.flink.stream.pojo.OrderSkuQty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Get data as OrderItem, and aggregate it to OrderSkuQty.
 * AggregateFunction can maintain a Accumulator state that developer created at the initialization stage
 *
 * Another way optional way is add another map to transform OrderItem to OrderSkuQty then reduce
 *
 */
public class OrderSummaryWithAggregate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<OrderItem> orderItemDataStream = env.fromElements(
                "B001 Jack 1671537515000 3",
                "B002 Jack 1671537515000 2",
                "B001 Tom 1671537518000 1",
                "B001 Tom1 1671537518000 1",
                "B003 Jason 1671537519000 5",
                "B002 Jason 1671537519000 7",
                "B005 Jason 1671537519000 -1",
                "B001 Green 1671537539000 4"
        ).map(new OrderItemMap())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderItem>forMonotonousTimestamps()
                                .withTimestampAssigner((orderItem, timestamp) -> orderItem.getTimestamp().getTime()))
                .filter(new OrderItemFilter());
        orderItemDataStream.print();

        /*
        Map from Order Item to OrderSkuQty to do the reduce
        DataStream<OrderSkuQty> orderSkuQtyDataStream = orderItemDataStream
                .map(new OrderItemToSkuQtyMap())
                .keyBy(OrderSkuQty::getSkuNo)
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(60),Time.seconds(60)))
                //.window(GlobalWindows.create())
                .reduce(new OrderSkuQtyReduce());
        orderSkuQtyDataStream.print();
        */
        DataStream<OrderSkuQty> orderSkuQtyDataStream = orderItemDataStream
                .keyBy(OrderItem::getSkuNo)
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(60),Time.seconds(60)))
                //.window(GlobalWindows.create())
                .aggregate(new OrderSkuQtyAggregate());
        orderSkuQtyDataStream.print();

        env.execute("Order Item");
    }
}