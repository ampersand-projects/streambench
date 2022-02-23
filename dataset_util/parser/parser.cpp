#include <iostream>

#include "taxi.pb.h"

#include <taxi_data_parser.h>

using namespace std;

int main(int argc, char* argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc != 3) {
        cerr << "Usage: <dataset directory> <dataset_name>" << endl;
    }
    string dataset_dir = argv[1];
    string dataset_name = argv[2];

    if (dataset_name == "taxi_trip") {
        taxi_trip_data_parser parser(dataset_dir);
        parser.parse();
    } else if (dataset_name == "taxi_fare") {
        taxi_fare_data_parser parser(dataset_dir);
        parser.parse();
    } else {
        throw std::runtime_error("Unknown dataset name.");
    }
    
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
