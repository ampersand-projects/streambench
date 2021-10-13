package org.streambench;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

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

        stream.foreachRDD(new VoidFunction<JavaRDD<Event>>() {
            @Override
            public void call(JavaRDD<Event> rdd) {
                SparkSession spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate();
                JavaRDD<Row> rowRDD = rdd.map(new Function<Event, Row>() {
                    @Override
                    public Row call(Event event) {
                      Row row = RowFactory.create(event);
                      return row;
                    }
                  });
            
                List<StructField> fields = new ArrayList<>();
                StructField field1 = DataTypes.createStructField("StartTime", DataTypes.TimestampType, true);
                StructField field2 = DataTypes.createStructField("EndTime", DataTypes.TimestampType, true);
                StructField field3 = DataTypes.createStructField("Payload", DataTypes.DoubleType, true);
                fields.add(field1);
                fields.add(field2);
                fields.add(field3);
                StructType schema = DataTypes.createStructType(fields);
        
                Dataset<Row> eventDataFrame = spark.createDataFrame(rowRDD, schema);
                eventDataFrame.show();
            }
        });

        /*
        JavaDStream<Float> res = stream.map(new Function<Event, Float>() {
            @Override
            public Float call(Event event) throws Exception {
                return event.payload + 10;
            }
        });
        res.print();
        */

        ssc.start();
        ssc.awaitTermination();
    }
}
