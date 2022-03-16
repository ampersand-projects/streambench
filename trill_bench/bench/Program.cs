using System;
using System.Diagnostics;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Google.Protobuf;
using Stream;

namespace bench
{
    class Program
    {
        static void Main(string[] args)
        {
            string testcase = (args.Length > 0) ? args[0] : "normalize";
            long size = (args.Length > 1) ? long.Parse(args[1]) : 100000000;
            long period = 1;
            double time = 0;

            switch (testcase)
            {
                case "select":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .Select(e => e + 3)
                    );
                    break;
                case "where":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .Where(e => e > 0)
                    );
                    break;
                case "aggregate":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .TumblingWindowLifetime(1000 * period)
                            .Sum(e => e)
                    );
                    break;
                case "alterdur":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .AlterEventDuration(10 * period)
                    );
                    break;
                case "innerjoin":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), BenchUtil.DataFn(period, size), (stream,stream2) =>
                        stream
                            .Join(stream2, (left, right) => left + right)
                    );        
                    break;
                case "outerjoin":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), BenchUtil.DataFn(period, size), (stream, stream2) =>
                        stream
                            .FullOuterJoin(stream2, e => true, e => true, 
                                left => left, right => right, 
                                (left,right)=> left + right)
                    );
                    break;
                case "normalize":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .Normalize(10000)
                    );
                    break;
                case "fillmean":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .FillMean(10000, period)
                    );
                    break;
                case "resample":
                    long iperiod = 4;
                    long operiod = 5;
                    Func<IStreamable<Empty, float>> sig4 = () =>
                    {
                        return new TestObs(iperiod, size)
                            .ToStreamable()
                            .Cache();
                    };
                    time = BenchUtil.RunTest(sig4, stream =>
                        stream
                            .Resample(iperiod, operiod)
                    );
                    break;
                case "algotrading":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .AlgoTrading(50, 20, period)
                    );    
                    break;
                case "largeqty":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .LargeQty(10, period)
                    );    
                    break;
                case "rsi":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .RSI(14, period)
                    );       
                    break;
                case "pantom":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .PanTom(period)
                    );
                    break;
                case "kurtosis":
                    time = BenchUtil.RunTest(BenchUtil.VibrationDataFn(size), stream =>
                        stream
                            .Kurtosis(100)
                    );
                    break;
                case "taxi":
                    time = BenchUtil.RunTest(BenchUtil.TaxiRideDataFn(size),
                                   BenchUtil.TaxiFareDataFn(size),
                                   (stream, stream2) =>
                        stream
                            .Taxi(stream2, 300)
                    );
                    break;
                case "eg1":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .Eg1(10, 20)
                    );
                    break;
                case "eg2":
                    time = BenchUtil.RunTest(BenchUtil.DataFn(period, size), stream =>
                        stream
                            .Eg2(10, 20)
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
