using System;
using System.Collections.Generic;
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
            long duration = 100000;
            long period = 8;
            long gap_tol = 100;
            long window = 1000;
            double time = 0;
            string testcase = "normalize".ToLower();

            Func<IStreamable<Empty, float>> data = () =>
            {
                return new TestObs("test", duration, period)
                    .ToStreamable();
            };

            switch (testcase)
            {
                case "normalize":
                    time = RunTest(data, stream =>
                        stream
                            .Normalize(window)
                    );
                    break;

                case "fillconst":
                    time = RunTest(data, stream =>
                        stream
                            .FillConst(period, gap_tol, 0)
                    );
                    break;

                case "fillmean":
                    time = RunTest(data, stream =>
                        stream
                            .FillMean(window, period, gap_tol)
                    );
                    break;

                case "resample":
                    time = RunTest(data, stream =>
                        stream
                            .Resample(period, period / 2)
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
