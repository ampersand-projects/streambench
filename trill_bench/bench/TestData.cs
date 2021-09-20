using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.StreamProcessing;

namespace bench
{
    using test_t = StreamEvent<float>;

    public class TestObs : IObservable<test_t>
    {
        public long size;
        public long period;
        public List<test_t> data;

        public TestObs(long period, long size)
        {
            this.period = period;
            this.size = size;
            this.data = new List<test_t>();
            Sample();
        }

        private void Sample()
        {
            var rand = new Random();
            double range = 100.0;
            for (long i = 0; i < size; i++)
            {
                var payload = rand.NextDouble() * range - (range / 2);
                data.Add(StreamEvent.CreateInterval(i * period, (i + 1) * period, (float) payload));
            }
        }

        public IDisposable Subscribe(IObserver<test_t> observer)
        {
            return new Subscription(this, observer);
        }

        private sealed class Subscription : IDisposable
        {
            private readonly TestObs observable;
            private readonly IObserver<test_t> observer;

            public Subscription(TestObs observable, IObserver<test_t> observer)
            {
                this.observer = observer;
                this.observable = observable;
                ThreadPool.QueueUserWorkItem(
                    arg =>
                    {
                        this.Sample();
                        this.observer.OnCompleted();
                    });
            }

            private void Sample()
            {
                for (int i = 0; i < observable.data.Count; i++)
                {
                    this.observer.OnNext(observable.data[i]);
                }
            }

            public void Dispose()
            {
            }
        }
    }
}