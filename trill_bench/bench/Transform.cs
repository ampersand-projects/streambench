using System;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    public static class Streamable
    {
        /// <summary>
        /// Performs the 'Chop' operator to chop (partition) gap intervals across beat boundaries with gap tolerance.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="offset">Stream offset</param>
        /// <param name="period">Beat period to chop</param>
        /// <param name="gap_tol">Gap tolerance</param>
        /// <returns>Signal stream after gaps chopped</returns>
        public static IStreamable<TKey, TPayload> Chop<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long offset,
            long period,
            long gap_tol)
        {
            gap_tol = Math.Max(period, gap_tol);
            return source
                    .AlterEventDuration((s, e) => e - s + gap_tol)
                    .Multicast(t => t.ClipEventDuration(t))
                    .AlterEventDuration((s, e) => (e - s > gap_tol) ? period : e - s)
                    .Chop(offset, period)
                ;
        }

        /// <summary>
        /// Attach aggregate results back to events
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="sourceSelector">Selector for source stream</param>
        /// <param name="aggregate">Aggregate function</param>
        /// <param name="resultSelector">Result selector</param>
        /// <param name="window">Window size</param>
        /// <param name="period">Period</param>
        /// <param name="offset">Offset</param>
        /// <returns>Signal stream after attaching aggregate result</returns>
        public static IStreamable<TKey, TOutput> AttachAggregate<TKey, TPayload, TInput, TState, TResult, TOutput>(
            this IStreamable<TKey, TPayload> source,
            Func<IStreamable<TKey, TPayload>, IStreamable<TKey, TInput>> sourceSelector,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState, TResult>> aggregate,
            Expression<Func<TPayload, TResult, TOutput>> resultSelector,
            long window,
            long period,
            long offset = 0)
        {
            return source
                    .Multicast(s => s
                        .ShiftEventLifetime(offset)
                        .Join(sourceSelector(s)
                                .HoppingWindowLifetime(window, period, offset)
                                .Aggregate(aggregate),
                            resultSelector)
                        .ShiftEventLifetime(-offset)
                    );
        }

        /// <summary>
        /// Attach aggregate results back to events
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="sourceSelector">Selector for source stream</param>
        /// <param name="aggregate1">Aggregate function1</param>
        /// <param name="aggregate2">Aggregate function2</param>
        /// <param name="merger">Aggregate merger function</param>
        /// <param name="resultSelector">Result selector</param>
        /// <param name="window">Window size</param>
        /// <param name="period">Period</param>
        /// <param name="offset">Offset</param>
        /// <returns>Signal stream after attaching aggregate result</returns>
        public static IStreamable<TKey, TOutput> AttachAggregate
            <TKey, TPayload, TInput, TState1, TResult1, TState2, TResult2, TResult, TOutput>(
            this IStreamable<TKey, TPayload> source,
            Func<IStreamable<TKey, TPayload>, IStreamable<TKey, TInput>> sourceSelector,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TResult1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TResult2>> aggregate2,
            Expression<Func<TResult1, TResult2, TResult>> merger,
            Expression<Func<TPayload, TResult, TOutput>> resultSelector,
            long window,
            long period,
            long offset = 0)
        {
            return source
                    .Multicast(s => s
                        .ShiftEventLifetime(offset)
                        .Join(sourceSelector(s)
                                .HoppingWindowLifetime(window, period, offset)
                                .Aggregate(aggregate1, aggregate2, merger),
                            resultSelector)
                        .ShiftEventLifetime(-offset)
                    );
        }

        /// <summary>
        /// Resample signal from one frequency to a different one.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="iperiod">Period of input signal stream</param>
        /// <param name="operiod">Period of output signal stream</param>
        /// <param name="offset">Offset</param>
        /// <returns>Result (output) stream in the new signal frequency</returns>
        public static IStreamable<TKey, float> Resample<TKey>(
            this IStreamable<TKey, float> source,
            long iperiod,
            long operiod,
            long offset = 0)
        {
            return source
                    .Select((ts, val) => new {ts, val})
                    .Multicast(s => s
                        .Join(s.ShiftEventLifetime(iperiod),
                            (l, r) => new {st = l.ts, sv = l.val, et = r.ts, ev = r.val}
                        )
                    )
                    .Chop(offset, operiod)
                    .HoppingWindowLifetime(1, operiod)
                    .AlterEventDuration(operiod)
                    .Select((t, e) => ((e.ev - e.sv) * (t - e.st) / (e.et - e.st) + e.sv));
        }

        /// <summary>
        /// Normalize a signal using standard score.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="window">Normalization window</param>
        /// <returns>Normalized signal</returns>
        public static IStreamable<TKey, float> Normalize<TKey>(
            this IStreamable<TKey, float> source,
            long window)
        {
            return source
                    .AttachAggregate(
                        s => s,
                        w => w.Average(e => e),
                        w => w.StandardDeviation(e => e),
                        (avg, std) => new {avg, std = (float) std.Value},
                        (signal, zscore) => ((signal - zscore.avg) / zscore.std),
                        window, window, window - 1
                    );
        }

        /// <summary>
        /// Fill missing values with a constant.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="period">Period of input signal stream</param>
        /// <param name="gap_tol">Gap tolerance</param>
        /// <param name="fill_val">Filler value</param>
        /// <param name="offset">Offset</param>
        /// <returns>Signal after missing values filled with `val`</returns>
        public static IStreamable<TKey, float> FillConst<TKey>(
            this IStreamable<TKey, float> source,
            long period,
            long gap_tol,
            float fill_val,
            long offset = 0)
        {
            return source
                    .Select((ts, val) => new {ts, val})
                    .Chop(offset, period, gap_tol)
                    .Select((ts, e) => (ts == e.ts) ? e.val : fill_val);
        }

        /// <summary>
        /// Fill missing values with mean of historic values.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="window">Mean window</param>
        /// <param name="period">Period of input signal stream</param>
        /// <param name="offset">Offset</param>
        /// <returns>Signal after missing values filled with `val`</returns>
        public static IStreamable<TKey, float> FillMean<TKey>(
            this IStreamable<TKey, float> source,
            long window,
            long period,
            long offset = 0)
        {
            return source
                .Multicast(s => s
                    .TumblingWindowLifetime(window)
                    .Average(e => e)
                    .LeftOuterJoin(s.ShiftEventLifetime(window),
                        e => true, e => true,
                        l => l, (l, r) => r
                    )
                );
        }

        /// <summary>
        /// Computes two moving averages of different lengths and compares them
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="longwin">Size of the longer window</param>
        /// <param name="shortwin">Size of the shorter window</param>
        /// <param name="period">Period of each event</param>
        /// <returns>Stream of bools indicating whether the moving average of the shorter window exceeds that of the
        /// longer window</returns>
        public static IStreamable<TKey, bool> AlgoTrading<TKey>(
            this IStreamable<TKey, float> source,
            long longwin,
            long shortwin,
            long period)
        {
            return source
                .Multicast(t => t
                    .HoppingWindowLifetime(longwin, period)
                    .Average(e => e)
                    .AlterEventDuration(period)
                    .Join(t
                            .HoppingWindowLifetime(shortwin, period)
                            .Average(e => e)
                            .AlterEventDuration(period),
                        (smaLong, smaShort) => smaShort > smaLong
                    )
                );
        }

        /// <summary>
        /// Fraud detection query. Groups input (representing qty of purchase for a particular item) into sliding
        /// windows, then computes the average and stdev of each window. 
        /// If a new transaction has a quantity that exceeds the value of (moving average + 3 * stdev) in the previous
        /// window, that new transaction is reported as a potentially fraudulent transaction.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="window">Size of the window</param>
        /// <param name="period">Period of each event</param>
        /// <returns>Stream of floats giving the quantities of those transactions flagged as potentially frauds.</returns>
        public static IStreamable<TKey, bool> LargeQty<TKey>(
            this IStreamable<TKey, float> source,
            long window,
            long period)
        {
            return source
                .Multicast(s => s
                    .Join(s
                            .HoppingWindowLifetime(window, period)
                            .Aggregate(w => new ZScoreAgg())
                            .AlterEventDuration(period)
                            .ShiftEventLifetime(period),
                        (val, zscore) => (val > zscore.avg + 3 * zscore.stddev)
                    )
                );
        }

        /// <summary>
        /// Calculates the Relative Strenght Index (RSI) given an RSI period.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="RSIperiod">number of days over which the RSI is calculated.</param>
        /// <param name="period">Period of each event</param>
        /// <returns>Stream of floats ranging in value from 0 to 100 representing the RSI calculated for each day.</returns>
        public static IStreamable<TKey, float> RSI<TKey>(
            this IStreamable<TKey, float> source,
            long RSIperiod,
            long period)
        {
            return source
                .Multicast(s => s
                    .ShiftEventLifetime(period)
                    .Join(s, (left, right) => right - left)
                )
                .HoppingWindowLifetime(RSIperiod, period)
                .Aggregate(
                    w => w.Average(e => (e > 0) ? e : 0),
                    w => w.Average(e => (e < 0) ? -e : 0),
                    (increase, decrease) => 100 - 100 / (1 + increase / decrease)
                )
                .AlterEventDuration(period);
        }

        /// <summary> 
        /// Pan-Tompkins Algorithm. Detects QRS complexes of ECG signals. It is also known as 
        /// the peak detection algorithm. 
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="period">Period of each event</param>
        /// <returns> Stream of floats that showcases the peaks of the original stream </returns>
        public static IStreamable<TKey, float> Pantom<TKey>(
            this IStreamable<TKey, float> source,
            long period)
        {
            return source
                .HoppingWindowLifetime(13 * period, period)
                .Aggregate(w => new LowPassFilterAggregate())
                .HoppingWindowLifetime(33 * period, period)
                .Aggregate(w => new HighPassFilterAggregate())
                .HoppingWindowLifetime(5 * period, period)
                .Aggregate(w => new DeriveAggregate(period))
                .Select(e => e * e)
                .HoppingWindowLifetime(30 * period, period)
                .Average(e => e);
        }
    }
}
