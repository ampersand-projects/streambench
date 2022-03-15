#include <iostream>

#include <taxi_data_parser.h>
#include <vibration_data_parser.h>

using namespace std;

int main(int argc, char* argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc < 4 || argc % 3 != 1) {
        cerr << "Usage: [<dataset directory> <dataset_name> <size>]" << endl;
    }

    for (int i = 0; i < argc / 3; i++) {
        string dataset_dir = argv[1 + i * 3];
        string dataset_name = argv[2 + i * 3];
        int64_t size = stol(argv[3 + i * 3]);

        if (dataset_name == "taxi_trip") {
            taxi_trip_data_parser parser(dataset_dir, size);
            parser.parse();
        } else if (dataset_name == "taxi_fare") {
            taxi_fare_data_parser parser(dataset_dir, size);
            parser.parse();
        } else if (dataset_name == "vibration") {
            vibration_data_parser parser(dataset_dir, size);
            parser.parse();
        } else {
            throw runtime_error("Unknown dataset name.");
        }
    }
    
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
