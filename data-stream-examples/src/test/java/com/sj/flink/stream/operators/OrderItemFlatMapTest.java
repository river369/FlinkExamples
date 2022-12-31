package com.sj.flink.stream.operators;

import com.sj.flink.stream.pojo.OrderItem;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class OrderItemFlatMapTest {


    @Test
    public void testFlatmap() throws Exception {
        // instantiate your function
        OrderItemFlatMap orderItemFlatMap = new OrderItemFlatMap();
        List<OrderItem> out = new ArrayList<>();
        ListCollector<OrderItem> listCollector = new ListCollector<OrderItem>(out);
        orderItemFlatMap.flatMap("B001 Jack 1671537515000 3", listCollector);
        Assert.assertEquals("B001", out.get(0).getSkuNo());
        Assert.assertEquals(1671537515000l, out.get(0).getTimestamp().getTime());

        /*
        Collector<OrderItem> collector = mock(Collector.class);
        // call the methods that you have implemented
        orderItemFlatMapFunction.flatMap("B001 Jack 1671537515000 3", collector);
        //verify collector was called with the right output
        Mockito.verify(collector, times(1)).collect(result.);
         */
    }
}
