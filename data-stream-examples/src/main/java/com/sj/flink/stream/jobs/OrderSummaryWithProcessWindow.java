package com.sj.flink.stream.jobs;

import com.sj.flink.stream.operators.OrderItemFilter;
import com.sj.flink.stream.operators.OrderItemFlatMap;
import com.sj.flink.stream.operators.OrderSkuQtyProcessWindow;
import com.sj.flink.stream.pojo.OrderItem;
import com.sj.flink.stream.pojo.OrderSkuQty;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class OrderSummaryWithProcessWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<OrderItem> orderItemDataStream = env.fromElements(
                "B001 Jack 1671537515000 3",
                "B002 Jack 1671537515000 2",
                "B001 Tom 1671537518000 1",
                "B003 Jason 1671537519000 5",
                "B002 Jason 1671537519000 7",
                "B005 Jason 1671537519000 -1",
                "B001 Green 1671537539000 4"
        ).flatMap(new OrderItemFlatMap())
                .filter(new OrderItemFilter())
                //.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderItem>forMonotonousTimestamps()
                //.withTimestampAssigner((orderItem, timestamp) -> orderItem.getTimestamp().getTime()))
                 ;
        orderItemDataStream.print();

        DataStream<OrderSkuQty> orderSkuQtyDataStream = orderItemDataStream
                .keyBy(OrderItem::getSkuNo)
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                //.window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .window(SlidingProcessingTimeWindows.of(Time.seconds(60),Time.seconds(60)))
                //.window(GlobalWindows.create())
                .process(new OrderSkuQtyProcessWindow());//.apply(new OrderSkuQtyWindowFunction()); //going to be deprecated
        orderSkuQtyDataStream.print();

        env.execute("Order Item");
    }

}

/*
        DataStream<OrderItem> orderItemDataStream  =
                SourceFactory.socketText(env,"localhost", 9999)
                        .flatMap(new OrderItemFlatMapFunction())
                        .filter(new OrderItemFilterFunction());
        orderItemDataStream.print();


        DataStream<OrderItem> orderItemDataStream  =
                SourceFactory.textFile(env,"/Users/gaojianguo/tmp/flink/data.txt")
                        .flatMap(new OrderItemFlatMapFunction())
                        .filter(new OrderItemFilterFunction());
        orderItemDataStream.print();
 */