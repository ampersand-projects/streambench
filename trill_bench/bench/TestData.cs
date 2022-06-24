using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.StreamProcessing;

namespace bench
{
    using test_t = PartitionedStreamEvent<int, float>;

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
        }

        public abstract TestObs<T> Init();

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
        private int keys;

        public SynDataObs(long period, long size, int keys = 1) : base(period, size)
        {
            this.keys = keys;
        }
        public override TestObs<test_t> Init()
        {
            var rand = new Random();
            double range = 100.0;
            for (long i = 0; i < size; i++)
            {
                var payload = rand.NextDouble() * range - (range / 2);
                for (int k = 0; k < keys; k++)
                {
                    var e = PartitionedStreamEvent.CreateInterval(k, i * period, (i + 1) * period, (float) payload);
                    data.Add(e);   
                }
            }

            return this;
        }
    }
}