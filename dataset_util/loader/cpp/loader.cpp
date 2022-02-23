#include <iostream>

#include <data_loader.h>
#include <taxi_data_parser.h>

using namespace std;

int main(int argc, char** argv)
{
    data_loader<stream::taxi_fare> loader;
    while (true) {
        stream::taxi_fare fare;
        loader.load_data(fare);
        cout << fare << endl;
    }
    
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}