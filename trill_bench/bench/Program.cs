using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;

namespace bench
{
    class Program
    {
        static double RunTest<TPayload, TResult>(int threads, Func<IStreamable<Empty, TPayload>> data,
            Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TResult>> transform)
        {
            var streams = new IStreamable<Empty, TPayload>[threads];
            
            Parallel.For(0, threads, i =>
            {
                streams[i] = data();
            });

            var sw = new Stopwatch();
            sw.Start();
            Parallel.For(0, threads, i => {
                var s_obs = transform(streams[i]);
                s_obs
                    .ToStreamEventObservable()
                    .Wait();
            });

            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }
        
        static double RunTest<TPayload1, TPayload2, TResult>(int threads,
            Func<IStreamable<Empty, TPayload1>> data1, 
            Func<IStreamable<Empty, TPayload2>> data2,
            Func<IStreamable<Empty, TPayload1>, IStreamable<Empty, TPayload2>, IStreamable<Empty, TResult>> transform)
        {
            var streams = new IStreamable<Empty, TPayload1>[threads];
            var streams2 = new IStreamable<Empty, TPayload2>[threads];
            
            Parallel.For(0, threads, i =>
            {
                streams[i] = data1();
                streams2[i] = data2();
            });

            var sw = new Stopwatch();
            sw.Start();
            Parallel.For(0, threads, i => {
                var s_obs = transform(streams[i], streams2[i]);
                s_obs
                    .ToStreamEventObservable()
                    .Wait();
            });

            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        static void Main(string[] args)
        {
            string testcase = (args.Length > 0) ? args[0] : "select";
            long size = (args.Length > 1) ? long.Parse(args[1]) : 100000000;
            long period = 1;
            double time = 0;
            int threads = (args.Length > 2) ? int.Parse(args[2]) : 1;

            Func<IStreamable<Empty, float>> data = () =>
            {
                return new SynDataObs(period, size)
                    .ToStreamable()
                    .Cache();
            };

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
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .Select(e => e + 3)
                    );
                    break;
                case "where":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .Where(e => e > 0)
                    );
                    break;
                case "aggregate":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .TumblingWindowLifetime(1000 * period)
                            .Sum(e => e)
                    );
                    break;
                case "alterdur":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .AlterEventDuration(10 * period)
                    );
                    break;
                case "innerjoin":
                    time = RunTest(threads, DataFn(period, size), DataFn(period, size), (stream,stream2) =>
                        stream
                            .Join(stream2, (left, right) => left + right)
                    );        
                    break;
                case "outerjoin":
                    time = RunTest(threads, DataFn(period, size), DataFn(period, size), (stream, stream2) =>
                        stream
                            .FullOuterJoin(stream2, e => true, e => true, 
                                left => left, right => right, 
                                (left,right)=> left + right)
                    );
                    break;
                case "normalize":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .Normalize(10000)
                    );
                    break;
                case "fillmean":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .FillMean(10000, period)
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
                    time = RunTest(threads, sig4, stream =>
                        stream
                            .Resample(iperiod, operiod)
                    );
                    break;
                case "algotrading":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .AlgoTrading(50, 20, period)
                    );    
                    break;
                case "largeqty":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .LargeQty(10, period)
                    );    
                    break;
                case "rsi":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .RSI(14, period)
                    );       
                    break;
                case "pantom":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .PanTom(period)
                    );
                    break;
                case "kurtosis":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .Kurtosis(100)
                    );
                    break;
                case "taxi":
                    time = RunTest(threads, TaxiRideDataFn(period, size),
                                   TaxiFareDataFn(period, size),
                                   (stream, stream2) =>
                        stream
                            .Taxi(stream2, 300)
                    );
                    break;
                case "eg1":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .Eg1(10, 20)
                    );
                    break;
                case "eg2":
                    time = RunTest(threads, DataFn(period, size), stream =>
                        stream
                            .Eg2(10, 20)
                    );
                    break;
                case "yahoo":
                    time = RunTest(threads, YahooDataFn(period, size), stream =>
                        stream
                            .Yahoo(10, 1)
                    );
                    break;
                default:
                    Console.Error.WriteLine("Unknown benchmark combination {0}", testcase);
                    return;
            }
            Console.WriteLine("Benchmark: {0}, Time: {1:.###} sec", testcase, time);
        }
    }
}
