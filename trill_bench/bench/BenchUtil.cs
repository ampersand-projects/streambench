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

       public static double RunTest<TPayload1, TPayload2, TResult>(
            Func<Tuple<IStreamable<Empty, TPayload1>, IStreamable<Empty, TPayload2>>> data,
            Func<IStreamable<Empty, TPayload1>, IStreamable<Empty, TPayload2>, IStreamable<Empty, TResult>> transform)
        {
            var result = data();
            var stream = result.Item1;
            var stream2 = result.Item2;

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

        public static Func<Tuple<IStreamable<Empty, TaxiRide>, IStreamable<Empty, TaxiFare>>> TaxiDataFn(long s)
        {
            return () => {
                var taxi_ride_data = new TaxiRideData();
                var taxi_fare_data = new TaxiFareData();

                MessageParser<stream_event> parser = new MessageParser<stream_event>(() => new stream_event());
                for (int i = 0; i < s; i++)
                {
                    stream_event s_event = parser.ParseDelimitedFrom(Console.OpenStandardInput());
                    if (s_event.PayloadCase == stream_event.PayloadOneofCase.TaxiTrip) {
                        taxi_ride_data.LoadDataPoint(s_event);
                    } else if (s_event.PayloadCase == stream_event.PayloadOneofCase.TaxiFare) {
                        taxi_fare_data.LoadDataPoint(s_event);
                    } else {
                        Debug.Assert(false);
                    }
                }

                return Tuple.Create(
                    (IStreamable<Empty, TaxiRide>) taxi_ride_data.ToStreamable().Cache(),
                    (IStreamable<Empty, TaxiFare>) taxi_fare_data.ToStreamable().Cache()
                );
            };
        }

        public static Func<IStreamable<Empty, float>> VibrationDataFn(long s)
        {
            return () => new VibrationObs(s)
                .ToStreamable()
                .Cache();
        }
    }
}