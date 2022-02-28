#include <iostream>
#include <string>

#include <data_loader.h>
#include <taxi_data_parser.h>
#include <vibration_data_parser.h>

using namespace std;

template<typename T>
void print_data(data_loader<T> &loader)
{
    while (true) {
        T t;
        if (!loader.load_data(t)) {
            break;
        }
        cout << t << endl;
    }
}

int main(int argc, char** argv)
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    
    string dataset_name = "taxi_fare";
    if (argc > 1) {
        dataset_name = argv[1];
    }

    if (dataset_name == "taxi_fare") {
        data_loader<stream::taxi_fare> loader;
        print_data<stream::taxi_fare>(loader);
    } else if (dataset_name == "taxi_trip") {
        data_loader<stream::taxi_trip> loader;
        print_data<stream::taxi_trip>(loader);
    } else if (dataset_name == "vibration") {
        data_loader<stream::vibration> loader;
        print_data<stream::vibration>(loader);
    } else {
        throw runtime_error("Unknown dataset name.");
    }
    
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}