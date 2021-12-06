using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.StreamProcessing;

namespace bench
{
    public class TaxiDriver
    {
        public int medallion;
        public int hack_license;
        public string vendor_id;

        public TaxiDriver(int medallion, int hack_license, string vendor_id)
        {
            this.medallion = medallion;
            this.hack_license = hack_license;
            this.vendor_id = vendor_id;
        }

        public override bool Equals(object obj)
        {
            if (obj == null) {
                return false;
            }
            if (!(obj is TaxiDriver)) {
                return false;
            }
            return (this.medallion == ((TaxiDriver) obj).medallion)
                && (this.hack_license == ((TaxiDriver) obj).hack_license)
                && (this.vendor_id == ((TaxiDriver) obj).vendor_id);
        }

        public override int GetHashCode()
        {
            return medallion.GetHashCode() ^ hack_license.GetHashCode() ^ vendor_id.GetHashCode();
        }
    }

    public class TaxiRecord
    {
        public TaxiDriver driver;
        public DateTime pickup_datetime;

        public TaxiRecord(int medallion, int hack_license, string vendor_id, DateTime pickup_datetime)
        {
            this.driver = new TaxiDriver(medallion, hack_license, vendor_id);
            this.pickup_datetime = pickup_datetime;
        }

        public override bool Equals(object obj)
        {
            if (obj == null) {
                return false;
            }
            if (!(obj is TaxiRecord)) {
                return false;
            }
            return (this.driver.Equals(((TaxiRecord) obj).driver))
                && (this.pickup_datetime.Equals(((TaxiRecord) obj).pickup_datetime));
        }

        public override int GetHashCode()
        {
            return driver.GetHashCode() ^ pickup_datetime.GetHashCode();
        }
    }

    public class TaxiRide
    {
        public int medallion;
        public float trip_distance;

        public TaxiRide(int medallion, float trip_distance)
        {
            this.medallion = medallion;
            this.trip_distance = trip_distance;
        }
    }

    public class TaxiFare
    {
        public int medallion;
        public float total_amount;

        public TaxiFare(int medallion, float total_amount)
        {
            this.medallion = medallion;
            this.total_amount = total_amount;
        }
    }

    public class TaxiDrivers
    {
        public static List<TaxiDriver> drivers;

        static TaxiDrivers()
        {
            SampleDrivers();
        }

        public static void SampleDrivers()
        {
            TaxiDrivers.drivers = new List<TaxiDriver>();
            for (int i = 0; i < 1000; i++)
            {
                var driver = new TaxiDriver(i, i, "Vendor-" + i.ToString());
                TaxiDrivers.drivers.Add(driver);
            }
        }
    }

    public abstract class TaxiDataObs<T> : IObservable<T>
    {
        public string filename;
        public long batch;
        public long count;
        public long time;
        public List<T> data;

        public TaxiDataObs(string filename, long batch, long count, long time)
        {
            this.filename = filename;
            this.batch = batch;
            this.count = count;
            this.time = time;
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
            private readonly TaxiDataObs<T> observable;
            private readonly IObserver<T> observer;

            public Subscription(TaxiDataObs<T> observable, IObserver<T> observer)
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

    public class TaxiFareData : TaxiDataObs<StreamEvent<TaxiFare>>
    {
        public TaxiFareData(string filename, long batch, long count, long time) : base(filename, batch, count, time)
        {}
        
        public override void Sample()
        {
            for (int i = 0; i < batch; i++)
            {
                using(var reader = new StreamReader(filename))
                {
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        var values = line.Split(',');
                        var ts = i*time + long.Parse(values[0]);
                        var fare = float.Parse(values[1]);
                        for (int j = 0; j < count; j++)
                        {
                            data.Add(StreamEvent.CreateInterval(
                                ts, ts+1, new TaxiFare(j, fare))
                            );   
                        }
                    }
                }    
            }
        }
    }

    public class TaxiRideData : TaxiDataObs<StreamEvent<TaxiRide>>
    {
        public TaxiRideData(string filename, long batch, long count, long time) : base(filename, batch, count, time)
        {}

        public override void Sample()
        {
            for (int i = 0; i < batch; i++)
            {
                using(var reader = new StreamReader(filename))
                {
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        var values = line.Split(',');
                        var st = i * time + long.Parse(values[0]);
                        var et = i * time + long.Parse(values[1]);
                        var dist = float.Parse(values[2]);
                        for (int j = 0; j < count; j++)
                        {
                            data.Add(StreamEvent.CreateInterval(
                                st, et, new TaxiRide(j, dist))
                            );
                        }
                    }
                }    
            }
        }
    }
}