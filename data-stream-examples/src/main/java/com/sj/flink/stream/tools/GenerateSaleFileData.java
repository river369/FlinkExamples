package com.sj.flink.stream.tools;

import java.io.FileOutputStream;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class GenerateSaleFileData {
    public static void main(String[] args) throws Exception {
        ExecutorService threadPool = Executors.newCachedThreadPool();
        FileWorker normalFileWorker = new FileWorker('N');
        FileWorker lateFileWorker = new FileWorker('L');
        Future<Long> res1 = threadPool.submit(normalFileWorker);
        Future<Long> res2 = threadPool.submit(lateFileWorker);
        try {
            System.out.println("The final results are " + res1.get() + " " + res2.get());
        } finally {
            threadPool.shutdown();
        }
    }
}

class FileWorker implements Callable {
    char workerType;
    public FileWorker(char workerType){
        this.workerType = workerType;
    }

    @Override
    public Long call() throws Exception {

        long start = System.currentTimeMillis();
        FileOutputStream fileOutputStream = new FileOutputStream("/tmp/flink/outputSalesData_"+workerType+ "_" + start+".txt", true);
        boolean running = true;
        while(running) {
            Long timeStamp = System.currentTimeMillis();
            if(timeStamp - start > 30*1000) running = false;
            long sleep = 500;
            if (workerType == 'L') {
                timeStamp = timeStamp - 20 * 1000;
                sleep = 1000;
            }

            Random random = new Random();
            String output = workerType + " " + timeStamp + " " + random.nextInt(100)+"\n";
            System.out.println(output);
            fileOutputStream.write(output.getBytes());
            fileOutputStream.flush();

            Thread.sleep(sleep);
        }
        fileOutputStream.close();
        return 0L;
    }
}
