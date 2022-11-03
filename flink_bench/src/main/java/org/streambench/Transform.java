package org.streambench;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.streambench.Bench.Data;
import org.streambench.Utility.SmaAggregation;
import org.streambench.Utility.YahooAggregation;
import org.streambench.Utility.ZscoreAggregation;
import org.streambench.Utility.ConstKeySelector;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class Transform {
    public static class AlgoTradeResult {
        public long start_time;
        public long end_time;
        public Boolean buy;

        public AlgoTradeResult(long start_time, long end_time, Boolean buy) {
            this.start_time = start_time;
            this.end_time = end_time;
            this.buy = buy;
        }

        public String toString() {
            return "start_time: " + String.valueOf(start_time) + " end_time: " + String.valueOf(end_time) + " payload: "
                    + String.valueOf(buy);
        }
    }

    static DataStream<AlgoTradeResult> AlgoTrade(DataStream<Data> source, long shortwin, long longwin, long period) {
        DataStream<Data> smaShort = source
                .windowAll(SlidingEventTimeWindows.of(Time.milliseconds(shortwin),
                        Time.milliseconds(period)))
                .aggregate(new SmaAggregation());

        DataStream<Data> smaLong = source
                .windowAll(SlidingEventTimeWindows.of(Time.milliseconds(longwin),
                        Time.milliseconds(period)))
                .aggregate(new SmaAggregation());

        DataStream<AlgoTradeResult> buy = smaShort.join(smaLong).where(new KeySelector<Bench.Data, Long>() {
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
                .apply(new JoinFunction<Data, Data, AlgoTradeResult>() {
                    @Override
                    public AlgoTradeResult join(Data first, Data second) {
                        return new AlgoTradeResult(first.end_time, first.end_time, first.payload > second.payload);
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

    public static class YahooInteraction {
        public long start_time;
        public long end_time;
        public long userID;
        public long campaignID;
        public long event_type;

        public YahooInteraction(long start_time, long end_time, long userID, long campaignID, long event_type) {
            this.start_time = start_time;
            this.end_time = end_time;
            this.userID = userID;
            this.campaignID = campaignID;
            this.event_type = event_type;
        }

        public String toString() {
            return "start_time: " + String.valueOf(start_time) + " end_time: " + String.valueOf(end_time) + " userID: "
                    + String.valueOf(userID) + " campaignID: " + String.valueOf(campaignID) + " event_type: "
                    + String.valueOf(event_type);
        }
    }

    public static class ReducedYahooInteraction {
        public long start_time;
        public long end_time;
        public long campaignID;
        public long event_type;

        public ReducedYahooInteraction(long start_time, long end_time, long campaignID, long event_type) {
            this.start_time = start_time;
            this.end_time = end_time;
            this.campaignID = campaignID;
            this.event_type = event_type;
        }

        public String toString() {
            return "start_time: " + String.valueOf(start_time) + " end_time: " + String.valueOf(end_time) 
                    + " campaignID: " + String.valueOf(campaignID) + " event_type: " + String.valueOf(event_type);
        }
    }

    static DataStream<Data> Yahoo(DataStream<YahooInteraction> source, long win_size, long event_type, long period) {
        DataStream<Data> result = source
                    .filter(new FilterFunction<YahooInteraction>() {
                                public boolean filter(YahooInteraction data) {
                                return data.event_type == event_type;
                                }
                            })
                    .map(new MapFunction<YahooInteraction, ReducedYahooInteraction>() {
                                public ReducedYahooInteraction map(YahooInteraction data) {
                                return new ReducedYahooInteraction(data.start_time, data.end_time, 
                                                                   data.campaignID, data.event_type);
                                }
                            })
                    .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(win_size * period)))
                    .aggregate(new YahooAggregation());
        
        return result;
    }
}

