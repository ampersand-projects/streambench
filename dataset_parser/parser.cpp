#include <iostream>

#include "taxi_trip.pb.h"

#include <taxi_data_parser.h>

using namespace std;

int main(int argc, char* argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc != 2) {
        cerr << "Usage: <dataset directory>" << endl;
    }
    string dataset_dir = argv[1];

    taxi_trip_data_parser parser(dataset_dir);
    parser.parse();
    
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
