using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.StreamProcessing;

namespace bench
{
    using test_t = StreamEvent<double>;

    public class TestObs : IObservable<test_t>
    {
        public long duration;
        public long period;
        public string signal;

        public List<test_t> data;

        public TestObs(string signal, long duration, long period)
        {
            this.signal = signal;
            this.period = period;
            this.duration = duration;
            this.data = new List<test_t>();
            Sample();
        }

        private void Sample()
        {
            for (long i = 0; i < duration; i += period)
            {
                data.Add(StreamEvent.CreateInterval(i, i + period, (double) i));
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