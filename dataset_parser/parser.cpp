#include <iostream>

#include "taxi_trip.pb.h"

#include <data_parser.h>
#include <taxi/taxi_data_gen.h>

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

    taxi_trip_data_gen data_generator;
    data_parser<stream::taxi_trip> parser(file, &data_generator);
    parser.parse();

    file.close();
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
