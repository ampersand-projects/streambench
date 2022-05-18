#ifndef DATASET_UTIL_DATA_PRINTER_H_
#define DATASET_UTIL_DATA_PRINTER_H_

#include <iostream>

#include <stream_event.pb.h>

using namespace std;

ostream& operator<< (ostream& out, stream::vibration const& vibration)
{
    out << "vibration: ";
    out << "channel_1: " << vibration.channel_1() << ", ";
    out << "channel_2: " << vibration.channel_2();
    return out;
}

ostream& operator<< (ostream& out, stream::taxi_trip const& trip)
{
    out << "taxi_trip: ";
    out << "medallion: " << trip.medallion() << ", ";
    out << "hack_license: " << trip.hack_license() << ", ";
    out << "vendor_id: " << trip.vendor_id() << ", ";
    out << "rate_code: " << trip.rate_code() << ", ";
    out << "store_and_fwd_flag: " << trip.store_and_fwd_flag() << ", ";
    out << "passenger_count: " << trip.passenger_count() << ", ";
    out << "trip_time_in_secs: " << trip.trip_time_in_secs() << ", ";
    out << "trip_distance: " << trip.trip_distance() << ", ";
    out << "pickup_longitude: " << trip.pickup_longitude() << ", ";
    out << "pickup_latitude: " << trip.pickup_latitude() << ", ";
    out << "dropoff_longitude: " << trip.dropoff_longitude() << ", ";
    out << "dropoff_latitude: " << trip.dropoff_latitude();
    return out;
}

ostream& operator<< (ostream& out, stream::taxi_fare const& fare)
{
    out << "taxi_fare: ";
    out << "medallion: " << fare.medallion() << ", ";
    out << "hack_license: " << fare.hack_license() << ", ";
    out << "vendor_id: " << fare.vendor_id() << ", ";
    out << "payment_type: " << fare.payment_type() << ", ";
    out << "fare_amount: " << fare.fare_amount() << ", ";
    out << "surcharge: " << fare.surcharge() << ", ";
    out << "mta_tax: " << fare.mta_tax() << ", ";
    out << "tip_amount: " << fare.tip_amount() << ", ";
    out << "tolls_amount: " << fare.tolls_amount() << ", ";
    out << "total_amount: " << fare.total_amount();
    return out;
}

ostream& operator<< (ostream& out, stream::stream_event const& event)
{
    out << "(Partition: " << event.part_key() << ") ";
    out << "Event: [" << event.st() << ", " << event.et() << "]: ";
    if (event.has_taxi_trip()) {
        out << event.taxi_trip();
    } else if (event.has_taxi_fare()) {
        out << event.taxi_fare();
    } else if (event.has_vibration()) {
        out << event.vibration();
    } else {
        out << "Unknown payload";
    }
    return out;
}

#endif // DATASET_UTIL_DATA_PRINTER_H_
