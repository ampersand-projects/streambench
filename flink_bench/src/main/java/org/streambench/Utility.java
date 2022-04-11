package org.streambench;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.streambench.Bench.Data;

public class Utility {
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

    public static class SmaAggregation implements AggregateFunction<Data, Tuple2<Data, Long>, Data> {
        @Override
        public Tuple2<Data, Long> createAccumulator() {
            return new Tuple2<>(new Data((long) Integer.MAX_VALUE, 0, 0), 0L);
        }

        @Override
        public Tuple2<Data, Long> add(Data value, Tuple2<Data, Long> accumulator) {
            accumulator.f0.start_time = Math.min(accumulator.f0.start_time, value.start_time);
            accumulator.f0.end_time = Math.max(accumulator.f0.end_time, value.end_time);
            accumulator.f0.payload += value.payload;
            accumulator.f1 += 1;
            return accumulator;
        }

        @Override
        public Data getResult(Tuple2<Data, Long> accumulator) {
            return new Data(accumulator.f0.start_time, accumulator.f0.end_time,
                    accumulator.f0.payload / accumulator.f1);
        }

        @Override
        public Tuple2<Data, Long> merge(Tuple2<Data, Long> a, Tuple2<Data, Long> b) {
            a.f0.start_time = Math.min(a.f0.start_time, b.f0.start_time);
            a.f0.end_time = Math.max(a.f0.end_time, b.f0.end_time);
            a.f0.payload += b.f0.payload;
            a.f1 += b.f1;
            return a;
        }
    }

    public static class StdAggregation implements AggregateFunction<Data, Tuple3<Data, Float, Long>, Data> {
        @Override
        public Tuple3<Data, Float, Long> createAccumulator() {
            return new Tuple3<>(new Data((long) Integer.MAX_VALUE, 0, 0), 0f, 0L);
        }

        @Override
        public Tuple3<Data, Float, Long> add(Data value, Tuple3<Data, Float, Long> accumulator) {
            accumulator.f0.start_time = Math.min(accumulator.f0.start_time, value.start_time);
            accumulator.f0.end_time = Math.max(accumulator.f0.end_time, value.end_time);
            accumulator.f0.payload += value.payload;
            accumulator.f1 += (float)Math.pow(value.payload, 2);
            accumulator.f2 += 1;
            return accumulator;
        }

        @Override
        public Data getResult(Tuple3<Data, Float, Long> accumulator) {
            float sum_x = accumulator.f0.payload;
            float sum_x2 = accumulator.f1;
            long N = accumulator.f2;
            return new Data(accumulator.f0.start_time, accumulator.f0.end_time,
                    (float)Math.sqrt(sum_x2 / N - Math.pow(sum_x / N, 2)));
        }

        @Override
        public Tuple3<Data, Float, Long> merge(Tuple3<Data, Float, Long> a, Tuple3<Data, Float, Long> b) {
            a.f0.start_time = Math.min(a.f0.start_time, b.f0.start_time);
            a.f0.end_time = Math.max(a.f0.end_time, b.f0.end_time);
            a.f0.payload += b.f0.payload;
            a.f1 += b.f1;
            a.f2 += b.f2;
            return a;
        }
    }
}
