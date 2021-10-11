package org.streambench;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws InterruptedException {
        long period = 1;
        long size = 10000;

        SparkConf sparkConf = new SparkConf().setAppName("spark_bench");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        JavaReceiverInputDStream<Event> stream = ssc.receiverStream(new TestReceiver(period, size));

        stream.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
