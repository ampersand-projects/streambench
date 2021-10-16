package org.streambench;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.*;

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
    public static Dataset<Row> generateDf(Long size, Long period, SparkSession spark) {
        List<Row> list = new ArrayList<>();
        Random rand = new Random();
        double range = 100.0;
        for (int i = 0; i < size; i++) {
            double payload = rand.nextDouble() * range - range / 2;
            list.add(RowFactory.create(new Timestamp(i * period), new Timestamp((i + 1) * period), (float) payload));
        }
        StructType schema = DataTypes.createStructType(
                new StructField[] { DataTypes.createStructField("start_time", DataTypes.TimestampType, false),
                        DataTypes.createStructField("end_time", DataTypes.TimestampType, false),
                        DataTypes.createStructField("payload", DataTypes.FloatType, false) });

        return spark.createDataFrame(list, schema);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Test").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        /*
        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create(new Timestamp(0), new Timestamp(1), 10.0f));
        list.add(RowFactory.create(new Timestamp(1), new Timestamp(2), 11.0f));
        list.add(RowFactory.create(new Timestamp(2), new Timestamp(3), 12.0f));
        list.add(RowFactory.create(new Timestamp(3), new Timestamp(4), 13.0f));

        StructType schema = DataTypes.createStructType(
                new StructField[] { DataTypes.createStructField("start_time", DataTypes.TimestampType, false),
                        DataTypes.createStructField("end_time", DataTypes.TimestampType, false),
                        DataTypes.createStructField("payload", DataTypes.FloatType, false) });

        Dataset<Row> stream = spark.createDataFrame(list, schema);
        */
        long size = 10;
        long period = 1;
        Dataset<Row> stream = generateDf(size, period, spark);

        /*
         * Dataset<Row> select = stream .map(new Function<Row, Row> () {
         * 
         * @Override public Row call(Row row) throws Exception { return
         * RowFactory.create(row.getTimestamp(0), row.getTimestamp(1),
         * row.getFloat(2)+3.0f); }} );
         */
        // Dataset<Row> select = stream.withColumn("payload", functions.col("payload") +
        // functions.lit(3));
        Dataset<Row> select = stream.select(functions.col("start_time"), functions.col("end_time"),
                functions.col("payload").plus(3.0f));

        Dataset<Row> where = stream.filter("payload > 0");

        Dataset<Row> aggregate = stream.groupBy(functions.window(stream.col("start_time"), "2 milliseconds"))
                .sum("payload");

        Dataset<Row> alterdur = stream.select(functions.col("start_time"),
                functions.col("start_time").plus(functions.expr("INTERVAL 10 MILLISECONDS")), functions.col("payload"));

        Dataset<Row> stream2 = generateDf(size, period, spark);
        Dataset<Row> innerjoin = stream.join(stream2,"start_time");
        
        innerjoin.foreach((ForeachFunction<Row>) e -> System.out.println(e));
    }
}
