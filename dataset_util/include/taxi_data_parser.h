#ifndef DATASET_UTIL_TAXI_DATA_PARSER_H_
#define DATASET_UTIL_TAXI_DATA_PARSER_H_

#include <assert.h>

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/filesystem.hpp>

#include <util.h>

#include <csv_parser.h>

using namespace std;
using namespace boost::filesystem;

class taxi_data_parser : public csv_parser
{
private:
    enum TAXI_FARE_DATA_INDEX {
        TAXI_FARE_MEDALLION,
        TAXI_FARE_HACK_LICENSE,
        TAXI_FARE_VENDOR_ID,
        TAXI_FARE_PICKUP_DATETIME,
        TAXI_FARE_PAYMENT_TYPE,
        TAXI_FARE_FARE_AMOUNT,
        TAXI_FARE_SURCHARGE,
        TAXI_FARE_MTA_TAX,
        TAXI_FARE_TIP_AMOUNT,
        TAXI_FARE_TOLLS_AMOUNT,
        TAXI_FARE_TOTAL_AMOUNT
    };
    enum TAXI_TRIP_DATA_INDEX {
        TAXI_TRIP_MEDALLION,
        TAXI_TRIP_HACK_LICENSE,
        TAXI_TRIP_VENDOR_ID,
        TAXI_TRIP_RATE_CODE,
        TAXI_TRIP_STORE_AND_FWD_FLAG,
        TAXI_TRIP_PICKUP_DATETIME,
        TAXI_TRIP_DROPOFF_DATETIME,
        TAXI_TRIP_PASSENGER_COUNT,
        TAXI_TRIP_TRIP_TIME_IN_SECS,
        TAXI_TRIP_TRIP_DISTANCE,
        TAXI_TRIP_PICKUP_LONGITUDE,
        TAXI_TRIP_PICKUP_LATITUDE,
        TAXI_TRIP_DROPOFF_LONGITUDE,
        TAXI_TRIP_DROPOFF_LATITUDE
    };

    const vector<string> foil_folders = {
        "FOIL2010", "FOIL2011",
        "FOIL2012", "FOIL2013"
    };
    vector<string> file_name_prefixes;
    string &dataset_dir;
    boost::posix_time::ptime start_time;

    int taxi_trip_part_key;
    int taxi_fare_part_key;
    int part_key;

    void decode(csv_row &row, stream::stream_event *event) override
    {
        if (part_key == taxi_trip_part_key) {
            decode_taxi_trip(row, event);
        } else if (part_key == taxi_fare_part_key) {
            decode_taxi_fare(row, event);
        } else {
            assert(false && "Partition key is unrecgonized");
        }
    }

    void decode_taxi_trip(csv_row &row, stream::stream_event *event) {
        int64_t st = parse_datetime_to_seconds(row[TAXI_TRIP_PICKUP_DATETIME], start_time);
        int64_t et = parse_datetime_to_seconds(row[TAXI_TRIP_DROPOFF_DATETIME], start_time);
        int32_t medallion = stoi(row[TAXI_TRIP_MEDALLION]);
        int32_t hack_license = stoi(row[TAXI_TRIP_HACK_LICENSE]);
        string vendor_id = row[TAXI_TRIP_VENDOR_ID];
        int32_t rate_code = stoi(row[TAXI_TRIP_RATE_CODE]);
        bool store_and_fwd_flag = false;
        int32_t passenger_count = stoi(row[TAXI_TRIP_PASSENGER_COUNT]);
        int32_t trip_time_in_secs = stoi(row[TAXI_TRIP_TRIP_TIME_IN_SECS]);
        float trip_distance = stof_err_handle(row[TAXI_TRIP_TRIP_DISTANCE]);
        float pickup_longitude = stof_err_handle(row[TAXI_TRIP_PICKUP_LONGITUDE]);
        float pickup_latitude = stof_err_handle(row[TAXI_TRIP_PICKUP_LATITUDE]);
        float dropoff_longitude = stof_err_handle(row[TAXI_TRIP_DROPOFF_LONGITUDE]);
        float dropoff_latitude = stof_err_handle(row[TAXI_TRIP_DROPOFF_LATITUDE]);

        event->set_st(st);
        event->set_et(et);
        event->set_part_key(part_key);
        event->mutable_taxi_trip()->set_medallion(medallion);
        event->mutable_taxi_trip()->set_hack_license(hack_license);
        event->mutable_taxi_trip()->set_vendor_id(vendor_id);
        event->mutable_taxi_trip()->set_rate_code(rate_code);
        event->mutable_taxi_trip()->set_store_and_fwd_flag(store_and_fwd_flag);
        event->mutable_taxi_trip()->set_passenger_count(passenger_count);
        event->mutable_taxi_trip()->set_trip_time_in_secs(trip_time_in_secs);
        event->mutable_taxi_trip()->set_trip_distance(trip_distance);
        event->mutable_taxi_trip()->set_dropoff_longitude(dropoff_longitude);
        event->mutable_taxi_trip()->set_pickup_latitude(pickup_latitude);
        event->mutable_taxi_trip()->set_pickup_longitude(pickup_longitude);
        event->mutable_taxi_trip()->set_dropoff_latitude(dropoff_latitude);
    }

    void decode_taxi_fare(csv_row &row, stream::stream_event *event) {
        int64_t st = parse_datetime_to_seconds(row[TAXI_FARE_PICKUP_DATETIME], start_time);
        int64_t et = st + 1;
        int32_t medallion = stoi(row[TAXI_FARE_MEDALLION]);
        int32_t hack_license = stoi(row[TAXI_FARE_HACK_LICENSE]);
        string vendor_id = row[TAXI_FARE_VENDOR_ID];
        string payment_type = row[TAXI_FARE_PAYMENT_TYPE];
        float fare_amount = stof_err_handle(row[TAXI_FARE_FARE_AMOUNT]);
        float surcharge = stof_err_handle(row[TAXI_FARE_SURCHARGE]);
        float mta_tax = stof_err_handle(row[TAXI_FARE_MTA_TAX]);
        float tip_amount = stof_err_handle(row[TAXI_FARE_TIP_AMOUNT]);
        float tolls_amount = stof_err_handle(row[TAXI_FARE_TOLLS_AMOUNT]);
        float total_amount = stof_err_handle(row[TAXI_FARE_TOTAL_AMOUNT]);

        event->set_st(st);
        event->set_et(et);
        event->set_part_key(part_key);
        event->mutable_taxi_fare()->set_medallion(medallion);
        event->mutable_taxi_fare()->set_hack_license(hack_license);
        event->mutable_taxi_fare()->set_vendor_id(vendor_id);
        event->mutable_taxi_fare()->set_payment_type(payment_type);
        event->mutable_taxi_fare()->set_fare_amount(fare_amount);
        event->mutable_taxi_fare()->set_surcharge(surcharge);
        event->mutable_taxi_fare()->set_mta_tax(mta_tax);
        event->mutable_taxi_fare()->set_tip_amount(tip_amount);
        event->mutable_taxi_fare()->set_tolls_amount(tolls_amount);
        event->mutable_taxi_fare()->set_total_amount(total_amount);
    }

public:
    taxi_data_parser(
        string &dataset_name,
        string &dataset_dir, 
        int taxi_trip_part_key = 0,
        int taxi_fare_part_key = 1
    ) :
        dataset_dir(dataset_dir),
        start_time(boost::gregorian::date(1970, 1, 1)),
        taxi_trip_part_key(taxi_trip_part_key),
        taxi_fare_part_key(taxi_fare_part_key)
    {
        assert(taxi_trip_part_key != taxi_fare_part_key &&
            "Partition key for trip and fare data cannot be the same");

        if (dataset_name == "taxi_trip") {
            file_name_prefixes.push_back("trip_data_");
        } else if (dataset_name == "taxi_fare") {
            file_name_prefixes.push_back("trip_fare_");
        } else {
            file_name_prefixes.push_back("trip_data_");
            file_name_prefixes.push_back("trip_fare_");
        }
    }
    ~taxi_data_parser(){}

    bool parse() override {
        const path data_dir(dataset_dir);
        if (!is_directory(data_dir)) {
            cerr << "Directory " << dataset_dir << " does not exist." << endl;
            return false;
        }

        for (auto &foil_folder : foil_folders) {
            path foil_dir = data_dir / foil_folder;
            if (!is_directory(foil_dir)) {
                cerr << "Directory " << foil_dir << " is skipped because it does not exist" << endl;
                continue;
            }

            size_t i = 1;
            while (true) {
                bool file_exists = false;
                for (auto &file_name_prefix : file_name_prefixes) {
                    path trip_data_file = foil_dir / (file_name_prefix + std::to_string(i) + ".csv");
                    if (exists(trip_data_file)) {
                        file_exists = true;
                    } else {
                        continue;
                    }

                    if (file_name_prefix == "trip_data_") {
                        part_key = taxi_trip_part_key;
                    } else if (file_name_prefix == "trip_fare_") {
                        part_key = taxi_fare_part_key;
                    } else {
                        assert(false);
                    }
                    
                    this->parse_csv_file(trip_data_file.string());
                }
                i++;
                if (!file_exists) {
                    break;
                }
            }
        }

        return true;
    }
};

#endif // DATASET_UTIL_TAXI_DATA_PARSER_H_