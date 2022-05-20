using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.StreamProcessing;

namespace bench
{
    public class Interaction
    {
        public long userID;
        public long campaignID;
        public long event_type;

        public Interaction(long userID, long campaignID, long event_type)
        {
            this.userID = userID;
            this.campaignID = campaignID;
            this.event_type = event_type;
        }
    }
    public class YahooObs : IObservable<StreamEvent<Interaction>>
    {
        public long size;
        public long period;
        public List<StreamEvent<Interaction>> data;

        public YahooObs(long period, long size)
        {
            this.period = period;
            this.size = size;
            this.data = new List<StreamEvent<Interaction>>();
            Sample();
        }

        private void Sample()
        {
            var rand = new Random();
            for (long i = 0; i < size; i++)
            {
                long userID = rand.Next(1, 5);
                long campaignID = rand.Next(1, 5);
                long event_type = rand.Next(1, 5);
                var payload = new Interaction(userID, campaignID, event_type);

                data.Add(StreamEvent.CreateInterval(i * period, (i + 1) * period, payload));
            }
        }

        public IDisposable Subscribe(IObserver<StreamEvent<Interaction>> observer)
        {
            return new Subscription(this, observer);
        }

        private sealed class Subscription : IDisposable
        {
            private readonly YahooObs observable;
            private readonly IObserver<StreamEvent<Interaction>> observer;

            public Subscription(YahooObs observable, IObserver<StreamEvent<Interaction>> observer)
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