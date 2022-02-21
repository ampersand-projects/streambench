#ifndef DATASET_STREAMER_DATA_GEN_DATA_PARSER_H_
#define DATASET_STREAMER_DATA_GEN_DATA_PARSER_H_

#include <fstream>

#include <boost/date_time/local_time/local_time.hpp>

#include <google/protobuf/util/delimited_message_util.h>

using namespace std;

template<typename T>
class data_parser
{
private:
    fstream &file;
    virtual void gen_data(vector<string>&, T*) = 0;

protected:
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

public:
    data_parser(fstream &file) :
        file(file)
    {}

    bool parse() {
        string line;
        getline(file, line);

        vector<string> row;
        while (true) {
            if (!parse_csv_line(file, row)) {
                break;
            }

            T data;
            gen_data(row, &data);
            if (!google::protobuf::util::SerializeDelimitedToOstream(data, &cout)) {
                cerr << "Fail to serialize data into output stream" << endl;
                return false;
            }
            row.clear();
        }
        return true;
    }
};

#endif // DATASET_STREAMER_DATA_GEN_DATA_PARSER_H_
