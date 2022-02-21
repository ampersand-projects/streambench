#include <iostream>

#include "taxi_trip.pb.h"

#include <taxi_data.h>

using namespace std;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cerr << "Usage: <file name>" << endl;
    }

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    fstream file(argv[1]);
    if (!file.is_open()) {
        cerr << "Cannot open file " << argv[1] << endl;
        return 1;
    }

    taxi_trip_data_parser parser(file);
    parser.parse();

    file.close();
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
