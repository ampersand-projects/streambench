#include <iostream>

#include <taxi_data_parser.h>
#include <vibration_data_parser.h>

using namespace std;

int main(int argc, char* argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc != 3) {
        cerr << "Usage: <dataset_name> <dataset directory>" << endl;
        return 1;
    }

    string dataset_name = argv[1];
    string dataset_dir = argv[2];

    if (dataset_name.find("taxi") != string::npos) {
        taxi_data_parser parser(dataset_name, dataset_dir);
        parser.parse();
    } else if (dataset_name == "vibration") {
        vibration_data_parser parser(dataset_dir);
        parser.parse();
    } else {
        throw runtime_error("Unknown dataset name.");
    }
    
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
