package com.sj.flink.stream.tools;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class GenerateSaleKafkaData {
    public static void main(String[] args) throws Exception {
        ExecutorService threadPool = Executors.newCachedThreadPool();
        KafkaWorker normalFileWorker = new KafkaWorker('N');
        KafkaWorker lateFileWorker = new KafkaWorker('L');
        Future<Long> res1 = threadPool.submit(normalFileWorker);
        Future<Long> res2 = threadPool.submit(lateFileWorker);
        try {
            System.out.println("The final results are " + res1.get() + " " + res2.get());
        } finally {
            threadPool.shutdown();
        }
    }
}

class KafkaWorker implements Callable {
    char workerType;
    public KafkaWorker(char workerType){
        this.workerType = workerType;
    }

    @Override
    public Long call() throws Exception {
        boolean running = true;
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        long start = System.currentTimeMillis();
        while(running) {
            Long timeStamp = System.currentTimeMillis();
            if(timeStamp - start > 10*1000) running = false;
            long sleep = 500;
            if (workerType == 'L') {
                timeStamp = timeStamp - 20 * 1000;
                sleep = 1000;
            }

            Random random = new Random();
            String output = workerType + " " + timeStamp + " " + random.nextInt(100);
            System.out.println(output);
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("quickstart-events", output);
            producer.send(producerRecord);
            producer.flush();

            Thread.sleep(sleep);
        }
        producer.close();
        return 0L;
    }
}
