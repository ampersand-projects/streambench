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
        /// <param name="gap_tol">Gap tolerance</param>
        /// <param name="offset">Offset</param>
        /// <returns>Signal after missing values filled with `val`</returns>
        public static IStreamable<TKey, float> FillMean<TKey>(
            this IStreamable<TKey, float> source,
            long window,
            long period,
            long gap_tol,
            long offset = 0)
        {
            return source
                    .Multicast(s => s
                        .Select((ts, val) => new {ts, val})
                        .Join(s
                                .TumblingWindowLifetime(window)
                                .Average(e => e),
                            (e, avg) => new {e.ts, e.val, avg}
                        )
                    )
                    .AlterEventDuration(period)
                    .Chop(offset, period, gap_tol)
                    .Select((ts, e) => (ts == e.ts) ? e.val : e.avg)
                    .AlterEventDuration(period);
        }
    }
}