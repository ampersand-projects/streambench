#ifndef DATASET_STREAMER_DATA_GEN_DATA_GEN_H_
#define DATASET_STREAMER_DATA_GEN_DATA_GEN_H_

#include <boost/date_time/local_time/local_time.hpp>

using namespace std;
using namespace boost;

template<typename T>
class data_gen
{
public:
    data_gen(){}
    ~data_gen(){}
    virtual void gen_data(vector<string>&, T*) = 0;

    float stof_err_handle(string &str)
    {
        try { return stof(str); }
        catch (std::exception& e) { return 0.0f; }
    }

    int64_t parse_datetime_to_seconds(string &datetime, posix_time::ptime &start_time)
    {
        auto time = posix_time::time_from_string(datetime);
        auto diff = time - start_time;
        return diff.total_seconds();
    }
};

#endif // DATASET_STREAMER_DATA_GEN_DATA_GEN_H_