#ifndef DATASET_UTIL_DATA_PARSER_H_
#define DATASET_UTIL_DATA_PARSER_H_

#include <google/protobuf/util/delimited_message_util.h>

#include <stream_event.pb.h>

using namespace std;

class data_parser
{
protected:
    virtual bool parse() = 0;

    bool write_serialized_to_ostream(stream::stream_event &t)
    {
        if (!google::protobuf::util::SerializeDelimitedToOstream(t, &cout)) {
            cerr << "Fail to serialize data into output stream" << endl;
            return false;
        }
        return true;
    }

public:
    data_parser(){}
};

#endif // DATASET_UTIL_DATA_PARSER_H_
