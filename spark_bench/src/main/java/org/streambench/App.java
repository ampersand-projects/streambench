package org.streambench;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws InterruptedException {
        long period = 1;
        long size = 10;

        SparkConf sparkConf = new SparkConf().setAppName("spark_bench");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        JavaReceiverInputDStream<Event> stream = ssc.receiverStream(new TestReceiver(period, size));

        JavaDStream<Float> res = stream.map(new Function<Event, Float>() {
            @Override
            public Float call(Event event) throws Exception {
                return event.payload + 10;
            }
        });
        res.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
