using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;

namespace bench
{
    class Program
    {
        static double RunTest<TPayload, TResult>(Func<IStreamable<Empty, TPayload>> data,
            Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TResult>> transform, int threads = 1)
        {
            var streams = new List<IStreamable<Empty, TPayload>>();
            for (int i = 0; i < threads; i++)
            {
                streams.Add(data());
            }

            var sw = new Stopwatch();
            sw.Start();
            Parallel.For(0, threads, i =>
            {
                var s_obs = transform(streams[i]);
                s_obs
                    .ToStreamEventObservable()
                    .Wait();
            });
            sw.Stop();

            return sw.Elapsed.TotalSeconds;
        }
        
        static double RunTest<TPayload1, TPayload2, TResult>(
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

        static void Main(string[] args)
        {
            string testcase = (args.Length > 0) ? args[0] : "normalize";
            long size = (args.Length > 1) ? long.Parse(args[1]) : 100000000;
            int threads = (args.Length > 2) ? int.Parse(args[2]) : 1;
            long period = 1;
            double time = 0;

            Func<IStreamable<Empty, float>> DataFn(long p, long s)
            {
                return () => new SynDataObs(p, s)
                    .ToStreamable()
                    .Cache();
            }

            Func<IStreamable<Empty, TaxiFare>> TaxiFareDataFn(long p, long s)
            {
                return () => new TaxiFareData(p, s)
                    .ToStreamable()
                    .Cache();
            }

            Func<IStreamable<Empty, TaxiRide>> TaxiRideDataFn(long p, long s)
            {
                return () => new TaxiRideData(p, s)
                    .ToStreamable()
                    .Cache();
            }

            Func<IStreamable<Empty, Interaction>> YahooDataFn(long p, long s)
            {
                return () => new YahooObs(p, s)
                    .ToStreamable()
                    .Cache();
            }
            
            switch (testcase)
            {
                case "select":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .Select(e => e + 3)
                    );
                    break;
                case "where":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .Where(e => e > 0)
                    );
                    break;
                case "aggregate":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .TumblingWindowLifetime(1000 * period)
                            .Sum(e => e)
                    );
                    break;
                case "alterdur":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .AlterEventDuration(10 * period)
                    );
                    break;
                case "innerjoin":
                    time = RunTest(DataFn(period, size), DataFn(period, size), (stream,stream2) =>
                        stream
                            .Join(stream2, (left, right) => left + right)
                    );        
                    break;
                case "outerjoin":
                    time = RunTest(DataFn(period, size), DataFn(period, size), (stream, stream2) =>
                        stream
                            .FullOuterJoin(stream2, e => true, e => true, 
                                left => left, right => right, 
                                (left,right)=> left + right)
                    );
                    break;
                case "normalize":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .Normalize(10000),
                        threads
                    );
                    break;
                case "fillmean":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .FillMean(10000, period),
                        threads
                    );
                    break;
                case "resample":
                    long iperiod = 4;
                    long operiod = 5;
                    Func<IStreamable<Empty, float>> sig4 = () =>
                    {
                        return new SynDataObs(iperiod, size)
                            .ToStreamable()
                            .Cache();
                    };
                    time = RunTest(sig4, stream =>
                        stream
                            .Resample(iperiod, operiod),
                        threads
                    );
                    break;
                case "algotrading":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .AlgoTrading(50, 20, period),
                        threads
                    );    
                    break;
                case "largeqty":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .LargeQty(10, period),
                        threads
                    );    
                    break;
                case "rsi":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .RSI(14, period),
                        threads
                    );       
                    break;
                case "pantom":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .PanTom(period),
                        threads
                    );
                    break;
                case "kurtosis":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .Kurtosis(100),
                        threads
                    );
                    break;
                case "taxi":
                    time = RunTest(TaxiRideDataFn(period, size),
                                   TaxiFareDataFn(period, size),
                                   (stream, stream2) =>
                        stream
                            .Taxi(stream2, 300)
                    );
                    break;
                case "eg1":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .Eg1(10, 20)
                    );
                    break;
                case "eg2":
                    time = RunTest(DataFn(period, size), stream =>
                        stream
                            .Eg2(10, 20)
                    );
                    break;
                case "yahoo":
                    time = RunTest(YahooDataFn(period, size), stream =>
                        stream
                            .Yahoo(100, 1),
                        threads
                    );
                    break;
                default:
                    Console.Error.WriteLine("Unknown benchmark combination {0}", testcase);
                    return;
            }
            Console.WriteLine("Throughput(M/s), {0}, {1}, {2:.###}", testcase, threads, (size * threads) / (time * 1000000));
        }
    }
}
