#ifndef TILT_BENCH_INCLUDE_TILT_TAXI_H_
#define TILT_BENCH_INCLUDE_TILT_TAXI_H_

#include <fstream>

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _Taxi(_sym trip, _sym fare, int64_t w)
{
    auto wtrip = trip[_win(-w, 0)];
    auto wtrip_sym = _sym("wtrip", wtrip);
    auto wfare = fare[_win(-w, 0)];
    auto wfare_sym = _sym("wfare", wfare);

    auto joined = _Join(wtrip_sym, wfare_sym, [](_sym l, _sym r) {
        auto dist = l << 1;
        auto amount = r << 1;
        return _new(vector<Expr>{dist, amount});
    });
    auto joined_sym = _sym("joined", joined);

    auto acc = [](Expr s, Expr st, Expr et, Expr d) {
        auto dist = _get(s, 0);
        auto amount = _get(s, 1);

        return _new(vector<Expr>{dist + _get(d, 0), amount + _get(d, 1)});
    };
    auto sum = _red(joined_sym, _new(vector<Expr>{_f32(0), _f32(0)}), acc);
    auto sum_sym = _sym("sum", sum);

    auto tot_dist = sum_sym << 0;
    auto tot_amount = sum_sym << 1;
    auto avg = tot_amount / tot_dist;
    auto avg_sym = _sym("avg", avg);

    return _op(
        _iter(0, w),
        Params{ trip, fare },
        SymTable{
            {wtrip_sym, wtrip},
            {wfare_sym, wfare},
            {joined_sym, joined},
            {sum_sym, sum},
            {avg_sym, avg}
        },
        _true(),
        avg_sym);
}

string replace(string s, char c1, char c2)
{
    int l = s.length();

    for (int i = 0; i < l; i++) {
        if (s[i] == c1) s[i] = c2;
        else if (s[i] == c2) s[i] = c1;
    }
    return s;
}

struct TripData {
    int medallion;
    float trip_distance;
};

struct FareData {
    int medallion;
    float total_amount;
};

class TripDataset : public Dataset<TripData> {
public:
    TripDataset(string filename, int batch, long time) :
        filename(filename), batch(batch), time(time)
    {}

    void fill(region_t* reg) final
    {
        auto data = reinterpret_cast<TripData*>(reg->data);

        for (int i=0; i<batch; i++) {
            ifstream fin(filename);
            string line;

            while (fin >> line) {
                auto l = replace(line, ',', ' ');

                string st_str, et_str, dist_str;
                stringstream ssin(l);
                ssin >> st_str >> et_str >> dist_str;
                long st = i*time + stol(st_str);
                long et = i*time + stol(et_str);
                float dist = stof(dist_str);
                commit_null(reg, st);
                commit_data(reg, et);
                auto* ptr = reinterpret_cast<TripData*>(fetch(reg, et, get_end_idx(reg), sizeof(TripData)));
                ptr->medallion = 1;
                ptr->trip_distance = dist;
            }
        }
        commit_null(reg, batch*time);
    }

private:
    string filename;
    int batch;
    long time;
};

class FareDataset : public Dataset<FareData> {
public:
    FareDataset(string filename, int batch, long time) :
        filename(filename), batch(batch), time(time)
    {}

    void fill(region_t* reg) final
    {
        auto data = reinterpret_cast<TripData*>(reg->data);

        for (int i=0; i<batch; i++) {
            ifstream fin(filename);
            string line;

            while (fin >> line) {
                auto l = replace(line, ',', ' ');

                string st_str, amnt_str;
                stringstream ssin(l);
                ssin >> st_str >> amnt_str;
                long st = i*time + stol(st_str);
                long et = st+1;
                float amnt = stof(amnt_str);
                commit_null(reg, st);
                commit_data(reg, et);
                auto* ptr = reinterpret_cast<FareData*>(fetch(reg, et, get_end_idx(reg), sizeof(TripData)));
                ptr->medallion = 1;
                ptr->total_amount = amnt;
            }
        }
        commit_null(reg, batch*time);
    }

private:
    string filename;
    int batch;
    long time;
};

class TaxiBench : public Benchmark {
public:
    TaxiBench(int batch, int count, long time) :
        batch(batch), count(count), time(time)
    {}

private:
    Op query() final
    {
        auto trip_sym = _sym("trip", tilt::Type(types::STRUCT<int, float>(), _iter(0, -1)));
        auto fare_sym = _sym("fare", tilt::Type(types::STRUCT<int, float>(), _iter(0, -1)));
        return _Taxi(trip_sym, fare_sym, time);
    }

    void init() final
    {
        for (int i=0; i<count; i++) {
            trip_regs.push_back(create_reg<TripData>(1000000));
            fare_regs.push_back(create_reg<FareData>(1000000));
            out_regs.push_back(create_reg<float>(1000000));
            TripDataset trip_dataset("/home/anandj/data/code/streambench/data/taxi/trip.csv", batch, time);
            trip_dataset.fill(&trip_regs[i]);

            FareDataset fare_dataset("/home/anandj/data/code/streambench/data/taxi/fare.csv", batch, time);
            fare_dataset.fill(&fare_regs[i]);
        }
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*, region_t*)) addr;
        for (int i=0; i<count; i++) {
            query(0, batch * time, &out_regs[i], &trip_regs[i], &fare_regs[i]);
        }
    }

    void release() final
    {
        for (int i=0; i<count; i++) {
            release_reg(&trip_regs[i]);
            release_reg(&fare_regs[i]);
            release_reg(&out_regs[i]);
        }
    }

    vector<region_t> trip_regs;
    vector<region_t> fare_regs;
    vector<region_t> out_regs;

    int batch;
    int count;
    long time;
    int64_t w;
};

#endif  // TILT_BENCH_INCLUDE_TILT_TAXI_H_
