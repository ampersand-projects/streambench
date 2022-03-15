#ifndef DATASET_UTIL_DATA_PARSER_H_
#define DATASET_UTIL_DATA_PARSER_H_

#include <fstream>

#include <boost/date_time/local_time/local_time.hpp>

#include <google/protobuf/util/delimited_message_util.h>

using namespace std;

template<typename T>
class data_parser
{
private:
    int64_t count = 0;
    int64_t size;
protected:
    virtual bool parse() = 0;
    virtual void gen_data(vector<string>&, T*) = 0;

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

    bool parse_csv_file(fstream &file) {
        string line;
        getline(file, line);

        vector<string> row;
        while (count < size) {
            if (!parse_csv_line(file, row)) {
                break;
            }

            T data;
            gen_data(row, &data);
            if (!write_serialized_to_ostream(data)) {
                return false;
            }
            row.clear();
            count++;
        }
        if (count < size) {
            return true;
        } else {
            return false;
        }
    }

    bool write_serialized_to_ostream(T &t) {
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
    data_parser(int64_t size) :
        size(size)
    {}
};

#endif // DATASET_UTIL_DATA_PARSER_H_
