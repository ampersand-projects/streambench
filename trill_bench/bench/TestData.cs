using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.StreamProcessing;

namespace bench
{
    using test_t = StreamEvent<float>;

    public abstract class TestObs<T> : IObservable<T>
    {
        public long size;
        public long period;
        public List<T> data;

        public TestObs(long period, long size)
        {
            this.period = period;
            this.size = size;
            this.data = new List<T>();
            Sample();
        }

        public abstract void Sample();

        public IDisposable Subscribe(IObserver<T> observer)
        {
            return new Subscription(this, observer);
        }

        private sealed class Subscription : IDisposable
        {
            private readonly TestObs<T> observable;
            private readonly IObserver<T> observer;

            public Subscription(TestObs<T> observable, IObserver<T> observer)
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

    public class SynDataObs : TestObs<test_t>
    {
        public SynDataObs(long period, long size) : base(period, size)
        {}
        public override void Sample()
        {
            var rand = new Random();
            double range = 100.0;
            for (long i = 0; i < size; i++)
            {
                var payload = rand.NextDouble() * range - (range / 2);
                data.Add(StreamEvent.CreateInterval(i * period, (i + 1) * period, (float) payload));
            }
        }
    }
}