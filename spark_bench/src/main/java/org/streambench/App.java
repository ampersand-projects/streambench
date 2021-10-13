package org.streambench;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 */
class Event {
    Timestamp start_time;
    Timestamp end_time;
    Float payload;

    public Event(Timestamp start_time, Timestamp end_time, Float payload) {
        this.start_time = start_time;
        this.end_time = end_time;
        this.payload = payload;
    }
}

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Test")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create(new Timestamp(0), new Timestamp(1), 10.0f));
        list.add(RowFactory.create(new Timestamp(1), new Timestamp(2), 11.0f));
        list.add(RowFactory.create(new Timestamp(2), new Timestamp(3), 12.0f));
        list.add(RowFactory.create(new Timestamp(3), new Timestamp(4), 13.0f));

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("start_time", DataTypes.TimestampType, false),
                DataTypes.createStructField("end_time", DataTypes.TimestampType, false),
                DataTypes.createStructField("payload", DataTypes.FloatType, false)
        });

        Dataset<Row> stream = spark
                .createDataFrame(list, schema);

        Dataset<Row> res = stream
                .groupBy(functions.window(stream.col("start_time"), "2 milliseconds"))
                .sum("payload");

        res.foreach((ForeachFunction<Row>) e -> System.out.println(e));
    }
}
