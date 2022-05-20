package org.streambench;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.streambench.Bench.Data;
import org.streambench.Utility.SmaAggregation;
import org.streambench.Utility.ZscoreAggregation;
import org.streambench.Utility.ConstKeySelector;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class Transform {
    public static class BoolStream {
        public long start_time;
        public long end_time;
        public Boolean buy;

        public BoolStream(long start_time, long end_time, Boolean buy) {
            this.start_time = start_time;
            this.end_time = end_time;
            this.buy = buy;
        }

        public String toString() {
            return "start_time: " + String.valueOf(start_time) + " end_time: " + String.valueOf(end_time) + " payload: "
                    + String.valueOf(buy);
        }
    }

    static DataStream<BoolStream> AlgoTrade(DataStream<Data> source, long shortwin, long longwin, long period) {
        DataStream<Data> smaShort = source
                .windowAll(SlidingEventTimeWindows.of(Time.milliseconds(shortwin),
                        Time.milliseconds(period)))
                .aggregate(new SmaAggregation());

        DataStream<Data> smaLong = source
                .windowAll(SlidingEventTimeWindows.of(Time.milliseconds(longwin),
                        Time.milliseconds(period)))
                .aggregate(new SmaAggregation());

        DataStream<BoolStream> buy = smaShort.join(smaLong).where(new KeySelector<Bench.Data, Long>() {
            @Override
            public Long getKey(Data left) {
                return left.end_time;
            }
        }).equalTo(new KeySelector<Bench.Data, Long>() {
            @Override
            public Long getKey(Data right) {
                return right.end_time;
            }
        }).window(TumblingEventTimeWindows.of(Time.milliseconds(period)))
                .apply(new JoinFunction<Data, Data, BoolStream>() {
                    @Override
                    public BoolStream join(Data first, Data second) {
                        return new BoolStream(first.end_time, first.end_time, first.payload > second.payload);
                    }
                });
        return buy;
    }

    public static class ZScore {
        public long start_time;
        public long end_time;
        public float avg;
        public float std;

        public ZScore(long start_time, long end_time, float avg, float std) {
            this.start_time = start_time;
            this.end_time = end_time;
            this.avg = avg;
            this.std = std;
        }

        public String toString() {
            return "start_time: " + String.valueOf(start_time) + " end_time: " + String.valueOf(end_time) + " avg: "
                    + String.valueOf(avg) + " std: " + String.valueOf(std);
        }
    }

    static DataStream<Data> Normalization(DataStream<Data> source, long win_size) {
        DataStream<ZScore> stats = source.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(win_size)))
                .aggregate(new ZscoreAggregation());

        DataStream<Data> results = source.join(stats)
                .where(new ConstKeySelector<Data>())
                .equalTo(new ConstKeySelector<ZScore>())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(win_size)))
                .apply(new JoinFunction<Data, ZScore, Data>() {
                    @Override
                    public Data join(Data left, ZScore right) {
                        return new Data(left.start_time, left.end_time, (left.payload - right.avg) / right.std);
                    }
                });
        return results;
    }

    static DataStream<BoolStream> LargeQty(DataStream<Data> source, long win_size, long period) {
        DataStream<ZScore> stats = source.windowAll(SlidingEventTimeWindows.of(Time.milliseconds(win_size),
                Time.milliseconds(period))).aggregate(new ZscoreAggregation());
        
        DataStream<ZScore> shifted = stats.map(new MapFunction<ZScore, ZScore>() {
            public ZScore map(ZScore zscore) {
                return new ZScore(zscore.end_time, zscore.end_time + period, zscore.avg, zscore.std);
            }
        });

        DataStream<BoolStream> results = source.join(shifted)
                .where(new KeySelector<Bench.Data, Long>() {
                    @Override
                    public Long getKey(Data left) {
                        return left.end_time;
                    }
                })
                .equalTo(new KeySelector<ZScore, Long>() {
                    @Override
                    public Long getKey(ZScore right) {
                        return right.end_time;
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.milliseconds(2 * period), Time.milliseconds(period)))
                .apply(new JoinFunction<Data, ZScore, BoolStream>() {
                    @Override
                    public BoolStream join(Data left, ZScore right) {
                        return new BoolStream(left.start_time, left.end_time, left.payload > right.avg + 3 * right.std);
                    }
                });
        
        return results;
    }
}