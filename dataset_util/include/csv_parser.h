#ifndef DATASET_UTIL_CSV_PARSER_H_
#define DATASET_UTIL_CSV_PARSER_H_

#include <fstream>

#include <data_parser.h>

using namespace std;

class csv_parser : public data_parser
{
protected:
    typedef vector<string> csv_row;
    virtual void decode(csv_row&, stream::stream_event*) = 0;

    bool parse_csv_line(fstream &file, csv_row &row)
    {
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

    void parse_csv_file(string file_name)
    {
        cerr << "Begin parsing " << file_name << endl;

        std::fstream csv_file(file_name);
        string line;
        getline(csv_file, line);
        long line_cnt = 0;

        csv_row row;
        while (true) {
            if (!parse_csv_line(csv_file, row)) {
                break;
            }

            stream::stream_event data;
            decode(row, &data);
            if (!write_serialized_to_ostream(data)) {
                break;
            }
            row.clear();
            line_cnt++;
        }

        csv_file.close();
        cerr << "Parsing finished. Number of data points: " << line_cnt << endl;
    }

public:
    csv_parser(){}
};

#endif // DATASET_UTIL_CSV_PARSER_H_
