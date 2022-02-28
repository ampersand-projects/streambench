#ifndef DATASET_UTIL_DATA_LOADER_H_
#define DATASET_UTIL_DATA_LOADER_H_

#include <iostream>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>

using namespace std;

template<typename T>
class data_loader
{
private:
    google::protobuf::io::IstreamInputStream raw_in;
    google::protobuf::io::CodedInputStream coded_in;
public:
    data_loader() :
        raw_in(&cin),
        coded_in(&raw_in)
    {}
    ~data_loader(){}

    bool load_data(T& t) {
        bool clean_eof;
        if (!google::protobuf::util::ParseDelimitedFromCodedStream(&t, &coded_in, &clean_eof)) {
            if (!clean_eof) {
                cerr << "Fail to parse data from coded input stream." << endl;
            }
            return false;
        }
        return true;
    }
};

#endif // DATASET_UTIL_DATA_LOADER_H_