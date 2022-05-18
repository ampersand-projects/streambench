#ifndef DATASET_UTIL_VIBRATION_DATA_PARSER_H_
#define DATASET_UTIL_VIBRATION_DATA_PARSER_H_

#include <map>

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/filesystem.hpp>

#include <util.h>

#include <csv_parser.h>

using namespace std;
using namespace boost::filesystem;

class vibration_data_parser : public csv_parser
{
private:
    enum VIBRATION_DATA_INDEX {
        TIMESTAMP,
        CHANNEL_1,
        CHANNEL_2
    };

    string &dataset_dir;
    const map<string, char> folder_prefix_map = {
        {"1 Data collected from a healthy bearing", 'H'},
        {"2 Data collected from a bearing with inner race fault", 'I'},
        {"3 Data collected from a bearing with outer race fault", 'O'},
        {"4 Data collected from a bearing with ball fault", 'B'},
        {"5 Data collected from a bearing with a combination of faults", 'C'}
    };
    const vector<string> file_suffices = {
        "-A-1.csv", "-A-2.csv", "-A-3.csv",
        "-B-1.csv", "-B-2.csv", "-B-3.csv",
        "-C-1.csv", "-C-2.csv", "-C-3.csv",
        "-D-1.csv", "-D-2.csv", "-D-3.csv"
    };

    int part_key;

public:
    vibration_data_parser(string &dataset_dir, int part_key = 0) :
        dataset_dir(dataset_dir),
        part_key(part_key)
    {}
    ~vibration_data_parser(){}

    void decode(csv_row &row, stream::stream_event *event) override {
        int64_t st = stoi(row[TIMESTAMP]);
        int64_t et = st + 1;
        float channel_1 = stof_err_handle(row[CHANNEL_1]);
        float channel_2 = stof_err_handle(row[CHANNEL_2]);

        event->set_st(st);
        event->set_et(et);
        event->set_part_key(part_key);
        event->mutable_vibration()->set_channel_1(channel_1);
        event->mutable_vibration()->set_channel_2(channel_2);
    }

    bool parse() override {
        const path data_dir(dataset_dir);
        if (!is_directory(data_dir)) {
            cerr << "Directory " << dataset_dir << " does not exist." << endl;
            return false;
        }

        for (auto &pair : folder_prefix_map) {
            auto folder_name = pair.first;
            auto file_prefix = pair.second;

            path folder_dir = data_dir / folder_name;
            if (!is_directory(folder_dir)) {
                cerr << "Directory " << folder_dir << " is skipped because it does not exist" << endl;
                continue;
            }
            
            for (auto &file_suffix : file_suffices) {
                path data_file = folder_dir / (file_prefix + file_suffix);
                if (!exists(data_file)) {
                    cerr << "File " << data_file << " is skipped because it does not exist" << endl;
                    continue;
                }

                this->parse_csv_file(data_file.string());
            }
        }
        
        return true;
    }
};

#endif // DATASET_UTIL_VIBRATION_DATA_PARSER_H_