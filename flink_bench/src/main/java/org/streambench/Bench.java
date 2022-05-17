package org.streambench;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.streambench.Transform.BoolStream;
import org.streambench.Utility.SumAggregation;
import org.streambench.Utility.ConstKeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.JoinFunction;

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
        long size = (args.length > 1) ? Long.parseLong(args[1]) : 10000000;
        long period = 1;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Data> stream1 = streamGen(size, period, env);
        long win_size;

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
                        .where(new ConstKeySelector<Data>())
                        .equalTo((new ConstKeySelector<Data>()))
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(period)))
                        .apply(new JoinFunction<Data, Data, Data>() {
                            @Override
                            public Data join(Data first, Data second) {
                                return new Data(first.start_time, first.end_time, first.payload + second.payload);
                            }
                        });
                break;
            case "algotrade":
                long shortwin = 20, longwin = 50;
                DataStream<BoolStream> buy = Transform.AlgoTrade(stream1, shortwin, longwin, period);
                break;
            case "normalize":
                win_size = 10;
                DataStream<Data> result = Transform.Normalization(stream1, win_size);
                break;
            case "largeqty":
                win_size = 10;
                DataStream<BoolStream> largeQty = Transform.LargeQty(stream1, win_size, period);
                break;
            default:
                System.out.println("Unknown benchmark type");
        }

        env.execute();
    }
}
