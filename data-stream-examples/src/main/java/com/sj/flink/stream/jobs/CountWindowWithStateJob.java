package com.sj.flink.stream.jobs;

import com.sj.flink.stream.operators.CountWindowAverageRichFlatMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CountWindowWithStateJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long,Long>> dataStream= env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(2L, 7L),
                Tuple2.of(1L, 7L),Tuple2.of(3L, 4L), Tuple2.of(3L, 2L))
                .keyBy(value -> value.f0);

        dataStream.print();

        dataStream.flatMap(new CountWindowAverageRichFlatMap())
                .print();

        env.execute("Window WordCount");
    }
}
/*
./bin/flink run -c com.sj.flink.stream.WindowWordCount  /Users/gaojianguo/code/study/flink/river-1/data-stream-examples/target/data-stream-examples-1.0-SNAPSHOT.jar  127.0.0.1 ????
*/