package com.sj.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class SourceFactory {
    public static DataStreamSource socketText(StreamExecutionEnvironment env, String hostname, int port){
        return env.socketTextStream(hostname, port);
    }

    public static DataStreamSource textFile(StreamExecutionEnvironment env, String fileName){
        return env.readTextFile(fileName);
    }

    public static FileSource fileSource(StreamExecutionEnvironment env, String fileName){
        return FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path(fileName))
                .monitorContinuously(Duration.ofSeconds(1L))
                .build();
    }

    /**
     *
     * @param env
     * @param bootstrapServers  "127.0.0.1:9092"
     * @param topic "quickstart-events"
     * @return
     */
    public static KafkaSource kafkaSource(StreamExecutionEnvironment env, String bootstrapServers, String topic){
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                //.setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
