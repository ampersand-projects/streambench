package org.streambench;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.streambench.Bench.Data;
import org.streambench.Utility.SmaAggregation;
import org.apache.flink.api.common.functions.JoinFunction;
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
}
