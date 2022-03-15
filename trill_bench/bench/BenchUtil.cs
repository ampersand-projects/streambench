using System;
using System.Diagnostics;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Google.Protobuf;
using Stream;

namespace bench
{
    public class BenchUtil
    {
        public static double RunTest<TPayload, TResult>(Func<IStreamable<Empty, TPayload>> data,
            Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TResult>> transform)
        {
            var stream = data();

            var sw = new Stopwatch();
            sw.Start();
            var s_obs = transform(stream);

            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }
        
        public static double RunTest<TPayload1, TPayload2, TResult>(
            Func<IStreamable<Empty, TPayload1>> data1, 
            Func<IStreamable<Empty, TPayload2>> data2,
            Func<IStreamable<Empty, TPayload1>, IStreamable<Empty, TPayload2>, IStreamable<Empty, TResult>> transform)
        {
            var stream = data1();
            var stream2 = data2();

            var sw = new Stopwatch();
            sw.Start();
            var s_obs = transform(stream,stream2);

            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        public static Func<IStreamable<Empty, float>> DataFn(long p, long s)
        {
            return () => new TestObs(p, s)
                .ToStreamable()
                .Cache();
        }

        public static Func<IStreamable<Empty, TaxiFare>> TaxiFareDataFn(long p, long s)
        {
            return () => new TaxiFareData(p, s)
                .ToStreamable()
                .Cache();
        }

        public static Func<IStreamable<Empty, TaxiRide>> TaxiRideDataFn(long p, long s)
        {
            return () => new TaxiRideData(p, s)
                .ToStreamable()
                .Cache();
        }
    }
}
