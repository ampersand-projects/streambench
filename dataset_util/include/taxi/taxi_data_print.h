#ifndef DATASET_LOADER_TAXI_DATA_PRINT_H_
#define DATASET_LOADER_TAXI_DATA_PRINT_H_

#include <iostream>

#include <taxi_trip.pb.h>

using namespace std;

ostream& operator<< (ostream& out, stream::taxi_trip const& trip)
{
    out << "taxi_trip[" << trip.st() << ", " << trip.et() << "]: ";
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

#endif // DATASET_LOADER_TAXI_DATA_PRINT_H_