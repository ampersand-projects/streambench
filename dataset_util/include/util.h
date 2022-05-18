#ifndef DATASET_UTIL_UTIL_H_
#define DATASET_UTIL_UTIL_H_

#include <boost/date_time/local_time/local_time.hpp>

using namespace std;

float stof_err_handle(string &str)
{
    try { return stof(str); }
    catch (std::exception& e) { return 0.0f; }
}

int64_t parse_datetime_to_seconds(string &datetime, boost::posix_time::ptime &start_time)
{
    auto time = boost::posix_time::time_from_string(datetime);
    auto diff = time - start_time;
    return diff.total_seconds();
}

#endif // DATASET_UTIL_UTIL_H_