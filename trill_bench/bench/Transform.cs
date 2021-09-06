using System;

namespace Microsoft.StreamProcessing
{
    public static class Streamable
    {
        /// <summary>
        /// Resample signal from one frequency to a different one.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="iperiod">Period of input signal stream</param>
        /// <param name="operiod">Period of output signal stream</param>
        /// <param name="offset">Offset</param>
        /// <returns>Result (output) stream in the new signal frequency</returns>
        public static IStreamable<TKey, double> Resample<TKey>(
            this IStreamable<TKey, double> source,
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
        public static IStreamable<TKey, double> Normalize<TKey>(
            this IStreamable<TKey, double> source,
            long window)
        {
            return source
                    .Multicast(s => s
                        .Join(s
                            .TumblingWindowLifetime(window)
                            .Aggregate(
                                w => w.Average(e => e),
                                w => w.StandardDeviation(e=>e),
                                (avg, stddev) => new {avg, std = stddev.Value}
                            ),
                            (val, zscore) => ((val - zscore.avg) / zscore.std)
                        )
                    );
        }
        
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
        /// Fill missing values with a constant.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="period">Period of input signal stream</param>
        /// <param name="gap_tol">Gap tolerance</param>
        /// <param name="fill_val">Filler value</param>
        /// <param name="offset">Offset</param>
        /// <returns>Signal after missing values filled with `val`</returns>
        public static IStreamable<TKey, double> FillConst<TKey>(
            this IStreamable<TKey, double> source,
            long period,
            long gap_tol,
            float fill_val,
            long offset = 0)
        {
            return source
                    .Select((ts, val) => new {ts, val})
                    .Chop(offset, period, gap_tol)
                    .Select((ts, e) => (ts == e.ts) ? e.val : fill_val)
                ;
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
        public static IStreamable<TKey, double> FillMean<TKey>(
            this IStreamable<TKey, double> source,
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
                    .AlterEventDuration(period)
                ;
        }
    }
}