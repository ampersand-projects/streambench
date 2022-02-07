package org.streambench;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class Bench {
    public static class Data {
        public Long start_time;
        public Long end_time;
        public Float payload;
        public Integer key;

        public Data(long start_time, long end_time, float payload) {
            this.start_time = start_time;
            this.end_time = end_time;
            this.payload = payload;
            this.key = 0;
        }

        public Integer getKey() {
            return this.key;
        }

        public String toString() {
            return "start_time: " + this.start_time.toString() + " end_time: " + this.end_time.toString() + " payload: "
                    + this.payload.toString();
        }
    }

    public static DataStream<Data> streamGen(long size, long period, StreamExecutionEnvironment env) {
        ArrayList<Data> source = new ArrayList<Data>();
        Random rand = new Random();
        double range = 100.0;
        for (int i = 0; i < size; i++) {
            double payload = rand.nextDouble() * range - range / 2;
            source.add(new Data(i * period, (i + 1) * period, (float) payload));
        }
        DataStream<Data> stream = env.fromCollection(source);
        WatermarkStrategy<Data> wmStrategy = WatermarkStrategy.<Data>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.start_time);
        DataStream<Data> with_timestamp = stream.assignTimestampsAndWatermarks(wmStrategy);
        return with_timestamp;
    }

    public static void main(String[] args) throws Exception {
        String benchmark = (args.length > 0) ? args[0] : "select";
        long size = (args.length > 1) ? Long.parseLong(args[1]) : 1000000;
        long period = 1;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Data> stream1 = streamGen(size, period, env);

        switch (benchmark) {
            case "select":
                DataStream<Data> select = stream1.map(new MapFunction<Data, Data>() {
                    public Data map(Data data) {
                        return new Data(data.start_time, data.end_time, data.payload + 3.0f);
                    }
                });
                break;
            case "where":
                DataStream<Data> where = stream1.filter(new FilterFunction<Data>() {
                    public boolean filter(Data data) {
                        return data.payload > 0;
                    }
                });
                break;
            case "aggregate":
                DataStream<Data> aggregate = stream1
                        .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(1000 * period)))
                        .reduce(new ReduceFunction<Data>() {
                            public Data reduce(Data x, Data y) {
                                return new Data(x.start_time, y.end_time, x.payload + y.payload);
                            }
                        });
                break;
            case "alterdur":
                DataStream<Data> alterdur = stream1.map(new MapFunction<Data, Data>() {
                    public Data map(Data data) {
                        return new Data(data.start_time, data.start_time + 10 * period, data.payload);
                    }
                });
                break;
            case "innerjoin":
                DataStream<Data> stream2 = streamGen(size, period, env);
                DataStream<Data> innerjoin = stream1.keyBy(Data::getKey).intervalJoin(stream2.keyBy(Data::getKey))
                        .between(Time.milliseconds(-1), Time.milliseconds(1))
                        .upperBoundExclusive()
                        .lowerBoundExclusive()
                        .process(new ProcessJoinFunction<Data, Data, Data>() {

                            @Override
                            public void processElement(Data left, Data right, Context ctx, Collector<Data> out) {
                                out.collect(new Data(ctx.getLeftTimestamp(), ctx.getRightTimestamp(),
                                        left.payload + right.payload));
                            }
                        });
                break;
            default:
                System.out.println("Unknown benchmark type");
        }

        env.execute();
    }
}

