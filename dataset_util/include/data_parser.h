#ifndef DATASET_STREAMER_DATA_GEN_DATA_PARSER_H_
#define DATASET_STREAMER_DATA_GEN_DATA_PARSER_H_

#include <fstream>

#include <google/protobuf/util/delimited_message_util.h>

#include <data_gen.h>

using namespace std;

template<typename T>
class data_parser
{
private:
    fstream &file;
    data_gen<T> *data_generator;
public:
    data_parser(fstream &file, data_gen<T> *data_generator) :
        file(file),
        data_generator(data_generator)
    {}

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

    bool parse() {
        string line;
        getline(file, line);

        vector<string> row;
        while (true) {
            if (!parse_csv_line(file, row)) {
                break;
            }

            T data;
            data_generator->gen_data(row, &data);
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
