package com.sj.flink.stream.windows;

import com.sj.flink.stream.source.SourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * Window is defined as TumblingEventTimeWindows, length is 10 seconds
 * Water Mark is added in 2 second of windows delete time
 *
 L 1671537500000 1  //+0
 L 1671537509999 9  //+9.9
 L 1671537510000 10  //+10 calculated next window
 L 1671537511999 11  //+11.9
 L 1671537512000 12  //+12 trigger time for the 1st window
 L 1671537515000 15  //+15
 */
public class WaterMarkSalesData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // the interval to crate water mark, default is 200
        //env.getConfig().setAutoWatermarkInterval(200);

        DataStream<Tuple3<String, Long, Integer>> saleData =
                SourceFactory.socketText(env,"localhost", 9999)
                        .map(new ParseSaleData());

        DataStream<Tuple3<String, Long, Integer>> dataStream = saleData
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                                //.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withIdleness(Duration.ofSeconds(20))
                                .withTimestampAssigner((sale, timestamp) -> sale.f1))
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce((a,b) -> new Tuple3<>(a.f0+":::", a.f1, a.f2 + b.f2));
                //.sum(2);
        dataStream.print();

        env.execute("Window WordCount");
    }

    private static class ParseSaleData
            extends RichMapFunction<String, Tuple3<String, Long, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple3<String, Long, Integer> map(String record) {
            String[] data = record.replaceAll("\n", "").split(" ");
            if(data.length != 3) {
                data = new String[]{"x","0","0"};
            }
            SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
            long timeStamp = Long.parseLong(data[1]);
            System.out.println("Key:" + data[0] + ",EventTime: " + sdf.format(timeStamp)  + " " +  data[1] + " " +  data[2]);
            return new Tuple3<>(
                    String.valueOf(data[0]),
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2]));
        }
    }
}

