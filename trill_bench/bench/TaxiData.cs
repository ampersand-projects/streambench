using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.StreamProcessing;

namespace bench
{
    public class TaxiRide
    {
        public int medallion;
        public int hack_license;
        public string vendor_id;
        public int rate_code;
        public bool store_and_fwd_flag;
        public DateTime pickup_datetime;
        public DateTime dropoff_datetime;
        public int passenger_count;
        public int trip_time_in_secs;
        public int trip_distance;
        public int pickup_longitude;
        public int pickup_latitude;
        public int dropoff_longitude;
        public int dropoff_latitude;

        public TaxiRide(int medallion, int hack_license, string vendor_id, int rate_code,
                        bool store_and_fwd_flag, DateTime pickup_datetime, DateTime dropoff_datetime,
                        int passenger_count, int trip_time_in_secs, int trip_distance, int pickup_longitude,
                        int pickup_latitude, int dropoff_longitude, int dropoff_latitude)
        {
            this.medallion = medallion;
            this.hack_license = hack_license;
            this.vendor_id = vendor_id;
            this.rate_code = rate_code;
            this.store_and_fwd_flag = store_and_fwd_flag;
            this.pickup_datetime = pickup_datetime;
            this.dropoff_datetime = dropoff_datetime;
            this.passenger_count = passenger_count;
            this.trip_time_in_secs = trip_time_in_secs;
            this.trip_distance = trip_distance;
            this.pickup_longitude = pickup_longitude;
            this.pickup_latitude = pickup_latitude;
            this.dropoff_longitude = dropoff_longitude;
            this.dropoff_latitude = dropoff_latitude;
        }

    }

    public class TaxiFare
    {
        public int medallion;
        public int hack_license;
        public string vendor_id;
        public DateTime pickup_datetime;
        public string payment_type;
        public int fare_amount;
        public int surcharge;
        public int mta_tax;
        public int tip_amount;
        public int tolls_amount;
        public int total_amount;

        public TaxiFare(int medallion, int hack_license, string vendor_id, DateTime pickup_datetime,
                        string payment_type, int fare_amount, int surcharge, int mta_tax, int tip_amount,
                        int tolls_amount, int total_amount)
        {
            this.medallion = medallion;
            this.hack_license = hack_license;
            this.vendor_id = vendor_id;
            this.pickup_datetime = pickup_datetime;
            this.payment_type = payment_type;
            this.fare_amount = fare_amount;
            this.surcharge = surcharge;
            this.mta_tax = mta_tax;
            this.tip_amount = tip_amount;
            this.tolls_amount = tolls_amount;
            this.total_amount = total_amount;
        }
    }

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
        public long size;
        public long period;
        public List<T> data;
        public DateTime datetime_base;

        public TaxiDataObs(long period, long size)
        {
            this.period = period;
            this.size = size;
            this.data = new List<T>();
            this.datetime_base = new DateTime(2021, 10, 19, 16, 0, 0);
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
        public TaxiFareData(long period, long size) : base(period, size)
        {}
        public override void Sample()
        {
            var rand = new Random();
            for (int i = 0; i < size; i++)
            {
                var driver = TaxiDrivers.drivers[i % TaxiDrivers.drivers.Count];
                int rate_code = rand.Next(0, 100);
                bool store_and_fwd_flag = rand.Next(0, 100) > 50;
                DateTime pickup_datetime = this.datetime_base.AddMinutes(i * 10);
                string payment_type = "VISA";
                int fare_amount = rand.Next(0, 100);
                int surcharge = rand.Next(0, 100);
                int mta_tax = rand.Next(0, 100);
                int tip_amount = rand.Next(0, 100);
                int tolls_amount = rand.Next(0, 100);
                int total_amount = rand.Next(0, 100);

                var payload = new TaxiFare(
                    driver.medallion,
                    driver.hack_license,
                    driver.vendor_id,
                    pickup_datetime,
                    payment_type,
                    fare_amount,
                    surcharge,
                    mta_tax,
                    tip_amount,
                    tolls_amount,
                    total_amount
                );
                data.Add(StreamEvent.CreateInterval(i * period, (i + 1) * period, payload));
            }
        }
    }

    public class TaxiRideData : TaxiDataObs<StreamEvent<TaxiRide>>
    {
        public TaxiRideData(long period, long size) : base(period, size)
        {}
        public override void Sample()
        {
            var rand = new Random();
            for (int i = 0; i < size; i++)
            {
                var driver = TaxiDrivers.drivers[i % TaxiDrivers.drivers.Count];
                int rate_code = rand.Next(0, 100);
                bool store_and_fwd_flag = rand.Next(0, 100) > 50;
                DateTime pickup_datetime = this.datetime_base.AddMinutes(i * 10);
                DateTime dropoff_datetime = pickup_datetime.AddMinutes(5);
                int passenger_count = rand.Next(0, 4);
                int trip_time_in_secs = (int)(dropoff_datetime - pickup_datetime).TotalSeconds;
                int trip_distance = rand.Next(0, 100);
                int pickup_longitude = rand.Next(0, 100);
                int pickup_latitude = rand.Next(0, 100);
                int dropoff_longitude = rand.Next(0, 100);
                int dropoff_latitude = rand.Next(0, 100);
                
                var payload = new TaxiRide(
                    driver.medallion,
                    driver.hack_license,
                    driver.vendor_id,
                    rate_code,
                    store_and_fwd_flag,
                    pickup_datetime,
                    dropoff_datetime,
                    passenger_count,
                    trip_time_in_secs,
                    trip_distance,
                    pickup_longitude,
                    pickup_latitude,
                    dropoff_longitude,
                    dropoff_latitude
                );
                data.Add(StreamEvent.CreateInterval(i * period, (i + 1) * period, payload));
            }
        }
    }
}