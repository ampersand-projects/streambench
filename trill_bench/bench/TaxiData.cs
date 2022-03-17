using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading;
using Microsoft.StreamProcessing;
using Google.Protobuf;
using Stream;

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
        public int hack_license;
        public string vendor_id;
        public int rate_code;
        public bool store_and_fwd_flag;
        public DateTime pickup_datetime;
        public DateTime dropoff_datetime;
        public int passenger_count;
        public float trip_time_in_secs;
        public float trip_distance;
        public float pickup_longitude;
        public float pickup_latitude;
        public float dropoff_longitude;
        public float dropoff_latitude;

        public TaxiRide(int medallion, int hack_license, string vendor_id, int rate_code,
                        bool store_and_fwd_flag, DateTime pickup_datetime, DateTime dropoff_datetime,
                        int passenger_count, float trip_time_in_secs, float trip_distance, float pickup_longitude,
                        float pickup_latitude, float dropoff_longitude, float dropoff_latitude)
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

        public override string ToString() {
            return String.Format(
                "{{medallion: {0}, hack_license: {1}, vendor_id: {2}, rate_code: {3}, " +
                "store_and_fwd_flag: {4}, pickup_datetime: {5}, dropoff_datetime: {6}, " +
                "passenger_count: {7}, trip_time_in_secs: {8}, trip_distance: {9}, " +
                "pickup_longitude: {10}, pickup_latitude: {11}, dropoff_longitude: {12}, " +
                "dropoff_latitude: {13}}}",
                this.medallion, this.hack_license, this.vendor_id, this.rate_code,
                this.store_and_fwd_flag, this.pickup_datetime, this.dropoff_datetime,
                this.passenger_count, this.trip_time_in_secs, this.trip_distance,
                this.pickup_longitude, this.pickup_latitude, this.dropoff_longitude,
                this.dropoff_latitude
            );
        }
    }

    public class TaxiFare
    {
        public int medallion;
        public int hack_license;
        public string vendor_id;
        public DateTime pickup_datetime;
        public string payment_type;
        public float fare_amount;
        public float surcharge;
        public float mta_tax;
        public float tip_amount;
        public float tolls_amount;
        public float total_amount;

        public TaxiFare(int medallion, int hack_license, string vendor_id, DateTime pickup_datetime,
                        string payment_type, float fare_amount, float surcharge, float mta_tax, 
                        float tip_amount, float tolls_amount, float total_amount)
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

        public override string ToString() {
            return String.Format(
                "{{medallion: {0}, hack_license: {1}, vendor_id: {2}, pickup_datetime: {3}, " +
                "payment_type: {4}, fare_amount: {5}, surcharge: {6}, mta_tax: {7}, tip_amount: {8}, " + 
                "tolls_amount: {9}, total_amount: {10}}}",
                this.medallion, this.hack_license, this.vendor_id, this.pickup_datetime,
                this.payment_type, this.fare_amount, this.surcharge, this.mta_tax, this.tip_amount,
                this.tolls_amount, this.total_amount
            );
        }
    }

    public abstract class TaxiDataObs<T> : IObservable<T>
    {
        public List<T> data;
        public DateTime datetime_base;

        public TaxiDataObs()
        {
            this.data = new List<T>();
            this.datetime_base = new DateTime(1970, 1, 1, 0, 0, 0);
        }

        public abstract void LoadDataPoint(stream_event s_event);

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
        public TaxiFareData() : base()
        {}
        public override void LoadDataPoint(stream_event s_event)
        {
            long st = s_event.St;
            var payload = new TaxiFare(
                s_event.TaxiFare.Medallion,
                s_event.TaxiFare.HackLicense,
                s_event.TaxiFare.VendorId,
                this.datetime_base.AddSeconds(st),
                s_event.TaxiFare.PaymentType,
                s_event.TaxiFare.FareAmount,
                s_event.TaxiFare.Surcharge,
                s_event.TaxiFare.MtaTax,
                s_event.TaxiFare.TipAmount,
                s_event.TaxiFare.TollsAmount,
                s_event.TaxiFare.TotalAmount
            );
            data.Add(StreamEvent.CreateInterval(st, st + 1, payload));
        }
    }

    public class TaxiRideData : TaxiDataObs<StreamEvent<TaxiRide>>
    {
        public TaxiRideData() : base()
        {}
        public override void LoadDataPoint(stream_event s_event)
        {
            long st = s_event.St;
            long et = s_event.Et;
            Debug.Assert(s_event.PayloadCase == stream_event.PayloadOneofCase.TaxiTrip);
            var payload = new TaxiRide(
                s_event.TaxiTrip.Medallion,
                s_event.TaxiTrip.HackLicense,
                s_event.TaxiTrip.VendorId,
                s_event.TaxiTrip.RateCode,
                s_event.TaxiTrip.StoreAndFwdFlag,
                this.datetime_base.AddSeconds(st),
                this.datetime_base.AddSeconds(et),
                s_event.TaxiTrip.PassengerCount,
                s_event.TaxiTrip.TripTimeInSecs,
                s_event.TaxiTrip.TripDistance,
                s_event.TaxiTrip.PickupLongitude,
                s_event.TaxiTrip.PickupLatitude,
                s_event.TaxiTrip.DropoffLongitude,
                s_event.TaxiTrip.DropoffLatitude
            );
            data.Add(StreamEvent.CreateInterval(st, st + 1, payload));
        }
    }
}