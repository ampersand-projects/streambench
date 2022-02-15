package org.streambench;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.*;

public class Bench {
    public static class Data {
        public long start_time;
        public long end_time;
        public float payload;

        public Data(long start_time, long end_time, float payload) {
            this.start_time = start_time;
            this.end_time = end_time;
            this.payload = payload;
        }
        
        public String toString() {
            return "start_time: " + String.valueOf(start_time) + " end_time: " + String.valueOf(end_time) + " payload: "
                    + String.valueOf(payload);
        }
    }

    private static class ConstKeySelector implements KeySelector<Data, Integer> {
        @Override
        public Integer getKey(Data value) {
            return 0;
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

    public static class SumAggregation implements AggregateFunction<Data, Data, Data> {
        @Override
        public Data createAccumulator() {
            return new Data((long) Integer.MAX_VALUE, 0, 0);
        }

        @Override
        public Data add(Data value, Data accumulator) {
            accumulator.start_time = Math.min(accumulator.start_time, value.start_time);
            accumulator.end_time = Math.max(accumulator.end_time, value.end_time);
            accumulator.payload += value.payload;
            return accumulator;
        }

        @Override
        public Data getResult(Data accumulator) {
            return new Data(accumulator.start_time, accumulator.end_time, accumulator.payload);
        }

        @Override
        public Data merge(Data a, Data b) {
            a.start_time = Math.min(a.start_time, b.start_time);
            a.end_time = Math.max(a.end_time, b.end_time);
            a.payload += b.payload;
            return a;
        }
    }

    public static void main(String[] args) throws Exception {
        String benchmark = (args.length > 0) ? args[0] : "select";
        long size = (args.length > 1) ? Long.parseLong(args[1]) : 10000000;
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
                        .aggregate(new SumAggregation());
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
                DataStream<Data> innerjoin = stream1.join(stream2)
                        .where(new ConstKeySelector())
                        .equalTo((new ConstKeySelector()))
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(1)))
                        .apply(new JoinFunction<Data, Data, Data>() {
                            @Override
                            public Data join(Data first, Data second) {
                                return new Data(first.start_time, first.end_time, first.payload + second.payload);
                            }
                        });
                break;
                
            default:
                System.out.println("Unknown benchmark type");
        }

        env.execute();
    }
}
