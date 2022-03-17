#ifndef DATASET_UTIL_DATA_PARSER_H_
#define DATASET_UTIL_DATA_PARSER_H_

#include <fstream>

#include <boost/date_time/local_time/local_time.hpp>

#include <google/protobuf/util/delimited_message_util.h>

#include <stream_event.pb.h>

using namespace std;

class data_parser
{
protected:
    virtual bool parse() = 0;
    virtual void gen_data(vector<string>&, stream::stream_event*, int flag = 0) = 0;

    bool parse_csv_line(fstream &file, vector<string> &row) {
        string line;

		if (getline(file, line)) {
			string word;
			stringstream ss(line);
	
			while (getline(ss, word, ',')) {
				row.push_back(word);
            }

			return true;
		}
		return false;
    }

    void parse_csv_file(fstream &file, int flag = 0) {
        string line;
        getline(file, line);

        vector<string> row;
        while (true) {
            if (!parse_csv_line(file, row)) {
                break;
            }

            stream::stream_event data;
            gen_data(row, &data, flag);
            if (!write_serialized_to_ostream(data)) {
                break;
            }
            row.clear();
        }
    }

    bool write_serialized_to_ostream(stream::stream_event &t) {
        if (!google::protobuf::util::SerializeDelimitedToOstream(t, &cout)) {
            cerr << "Fail to serialize data into output stream" << endl;
            return false;
        }
        return true;
    }

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

public:
    data_parser(){}
};

#endif // DATASET_UTIL_DATA_PARSER_H_
