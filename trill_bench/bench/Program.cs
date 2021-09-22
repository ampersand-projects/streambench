using System;
using System.Diagnostics;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;

namespace bench
{
    class Program
    {
        static double RunTest<TPayload, TResult>(Func<IStreamable<Empty, TPayload>> data,
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

        static void Main(string[] args)
        {
            string testcase = (args.Length > 0) ? args[0] : "normalize";
            long size = (args.Length > 1) ? long.Parse(args[1]) : 100000000;
            long period = 1;
            double time = 0;

            Func<IStreamable<Empty, float>> data = () =>
            {
                return new TestObs(period, size)
                    .ToStreamable()
                    .Cache();
            };

            var stream2 = data(); 
            switch (testcase)
            {
                case "select":
                    time = RunTest(data, stream =>
                        stream
                            .Select(e => e + 3)
                    );
                    break;
                case "where":
                    time = RunTest(data, stream =>
                        stream
                            .Where(e => e > 0)
                    );
                    break;
                case "aggregate":
                    time = RunTest(data, stream =>
                        stream
                            .TumblingWindowLifetime(10 * period)
                            .Sum(e => e)
                    );
                    break;
                case "alterduration":
                    time = RunTest(data, stream =>
                        stream
                            .AlterEventDuration(10 * period)
                    );
                    break;
                case "innerjoin":
                    time = RunTest(data, stream =>
                        stream
                            .Join(stream2, (left, right) => left + right)
                    );        
                    break;
                case "outerjoin":
                    time = RunTest(data, stream =>
                        stream
                            .FullOuterJoin(stream2, e => e, e => e, 
                                left => left, right => right, 
                                (left,right)=> left + right)
                    );
                    break;
                case "normalize":
                    time = RunTest(data, stream =>
                        stream
                            .Normalize(10000)
                    );
                    break;
                case "fillmean":
                    time = RunTest(data, stream =>
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
                    time = RunTest(sig4, stream =>
                        stream
                            .Resample(iperiod, operiod)
                    );
                    break;
                case "algotrading":
                    time = RunTest(data, stream =>
                        stream
                            .AlgoTrading(50, 20, period)
                    );    
                    break;
                case "largeqty":
                    time = RunTest(data, stream =>
                        stream
                            .LargeQty(10, period)
                    );    
                    break;
                case "rsi":
                    time = RunTest(data, stream =>
                        stream
                            .RSI(14, period)
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
