#ifndef DATASET_LOADER_TAXI_DATA_LOADER_H_
#define DATASET_LOADER_TAXI_DATA_LOADER_H_

#include <string>

#include <boost/date_time/gregorian/gregorian.hpp>

#include <taxi_trip.pb.h>

#include <data_parser.h>

using namespace std;

class taxi_trip_data_parser : public data_parser<stream::taxi_trip>
{
private:
    enum TAXI_DATA_INDEX {
        MEDALLION,
        HACK_LICENSE,
        VENDOR_ID,
        RATE_CODE,
        STORE_AND_FWD_FLAG,
        PICKUP_DATETIME,
        DROPOFF_DATETIME,
        PASSENGER_COUNT,
        TRIP_TIME_IN_SECS,
        TRIP_DISTANCE,
        PICKUP_LONGITUDE,
        PICKUP_LATITUDE,
        DROPOFF_LONGITUDE,
        DROPOFF_LATITUDE
    };
    boost::posix_time::ptime start_time;

public:
    taxi_trip_data_parser(fstream &file) :
        data_parser<stream::taxi_trip>(file),
        start_time(boost::gregorian::date(1970, 1, 1))
    {}
    ~taxi_trip_data_parser(){}

    void gen_data(vector<string> &row, stream::taxi_trip *trip) override {
        int64_t st = this->parse_datetime_to_seconds(row[PICKUP_DATETIME], start_time);
        int64_t et = this->parse_datetime_to_seconds(row[DROPOFF_DATETIME], start_time);
        int32_t medallion = stoi(row[MEDALLION]);
        int32_t hack_license = stoi(row[HACK_LICENSE]);
        string vendor_id = row[VENDOR_ID];
        int32_t rate_code = stoi(row[RATE_CODE]);
        bool store_and_fwd_flag = false;
        int32_t passenger_count = stoi(row[PASSENGER_COUNT]);
        int32_t trip_time_in_secs = stoi(row[TRIP_TIME_IN_SECS]);
        float trip_distance = this->stof_err_handle(row[TRIP_DISTANCE]);
        float pickup_longitude = this->stof_err_handle(row[PICKUP_LONGITUDE]);
        float pickup_latitude = this->stof_err_handle(row[PICKUP_LATITUDE]);
        float dropoff_longitude = this->stof_err_handle(row[DROPOFF_LONGITUDE]);
        float dropoff_latitude = this->stof_err_handle(row[DROPOFF_LATITUDE]);

        trip->set_st(st);
        trip->set_et(et);
        trip->set_medallion(medallion);
        trip->set_hack_license(hack_license);
        trip->set_vendor_id(vendor_id);
        trip->set_rate_code(rate_code);
        trip->set_store_and_fwd_flag(store_and_fwd_flag);
        trip->set_passenger_count(passenger_count);
        trip->set_trip_time_in_secs(trip_time_in_secs);
        trip->set_trip_distance(trip_distance);
        trip->set_dropoff_longitude(dropoff_longitude);
        trip->set_pickup_latitude(pickup_latitude);
        trip->set_pickup_longitude(pickup_longitude);
        trip->set_dropoff_latitude(dropoff_latitude);
    }
};

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

#endif // DATASET_LOADER_TAXI_DATA_LOADER_H_