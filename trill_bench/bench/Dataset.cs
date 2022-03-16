using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.StreamProcessing;
using Google.Protobuf;
using Stream;

namespace bench
{
    using test_t = StreamEvent<float>;

    public abstract class DatasetObs : IObservable<test_t>
    {
        public long size;
        public List<test_t> data;

        public DatasetObs(long size)
        {
            this.size = size;
            this.data = new List<test_t>();
        }

        public abstract void LoadDataPoint();

        public void LoadData()
        {
            for (int i = 0; i < size; i++)
            {
                this.LoadDataPoint();
            }
        }

        public IDisposable Subscribe(IObserver<test_t> observer)
        {
            return new Subscription(this, observer);
        }

        private sealed class Subscription : IDisposable
        {
            private readonly DatasetObs observable;
            private readonly IObserver<test_t> observer;

            public Subscription(DatasetObs observable, IObserver<test_t> observer)
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

    public class VibrationObs : DatasetObs
    {
        private MessageParser<vibration> parser;
        
        public override void LoadDataPoint()
        {
            vibration vibration = parser.ParseDelimitedFrom(Console.OpenStandardInput());
            long st = vibration.St;
            long et = vibration.Et;
            float payload = vibration.Payload.Channel1;
            data.Add(StreamEvent.CreateInterval(st, et, payload));
        }

        public VibrationObs(long size) : base(size)
        {
            this.parser = new MessageParser<vibration>(() => new vibration());
            LoadData();
        }
    }
}