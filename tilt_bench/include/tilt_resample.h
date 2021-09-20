#ifndef TILT_BENCH_INCLUDE_TILT_RESAMPLE_H_
#define TILT_BENCH_INCLUDE_TILT_RESAMPLE_H_

#include <numeric>

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _Pair(_sym in, int64_t iperiod)
{
    auto ev = in[_pt(0)];
    auto ev_sym = _sym("ev", ev);
    auto sv = in[_pt(-iperiod)];
    auto sv_sym = _sym("sv", sv);
    auto beat = _beat(_iter(0, iperiod));
    auto et = _cast(types::FLOAT32, beat[_pt(0)]);
    auto et_sym = _sym("et", et);
    auto st = et_sym - _f32(iperiod);
    auto st_sym = _sym("st", st);
    auto res = _new(vector<Expr>{st_sym, sv_sym, et_sym, ev_sym});
    auto res_sym = _sym("res", res);
    auto pair_op = _op(
        _iter(0, iperiod),
        Params{in, beat},
        SymTable{
            {st_sym, st},
            {sv_sym, sv},
            {et_sym, et},
            {ev_sym, ev},
            {res_sym, res},
        },
        _exists(sv_sym) && _exists(ev_sym),
        res_sym);
    return pair_op;
}

Op _Interpolate(_sym in, int64_t operiod)
{
    auto e = in[_pt(0)];
    auto e_sym = _sym("e", e);
    auto beat = _beat(_iter(0, operiod));
    auto t = _cast(types::FLOAT32, beat[_pt(0)]);
    auto t_sym = _sym("t", t);
    auto st = e_sym << 0;
    auto sv = e_sym << 1;
    auto et = e_sym << 2;
    auto ev = e_sym << 3;
    auto res = (((ev - sv) * (t_sym - st)) / (et - st)) + sv;
    auto res_sym = _sym("res", res);
    auto inter_op = _op(
        _iter(0, operiod),
        Params{in, beat},
        SymTable{
            {e_sym, e},
            {t_sym, t},
            {res_sym, res},
        },
        _exists(e_sym),
        res_sym);
    return inter_op;
}

Op _Resample(_sym in, int64_t iperiod, int64_t operiod, int64_t scale)
{
    auto win_size = scale * lcm(iperiod, operiod);
    auto win = in[_win(-win_size, 0)];
    auto win_sym = _sym("win", win);
    auto pair = _Pair(win_sym, iperiod);
    auto pair_sym = _sym("pair", pair);
    auto inter = _Interpolate(pair_sym, operiod);
    auto inter_sym = _sym("inter", inter);
    auto resample_op = _op(
        _iter(0, win_size),
        Params{in},
        SymTable{
            {win_sym, win},
            {pair_sym, pair},
            {inter_sym, inter},
        },
        _true(),
        inter_sym);
    return resample_op;
}

class ResampleBench : public Benchmark {
public:
    ResampleBench(dur_t iperiod, int64_t operiod, int64_t scale, int64_t size) :
        iperiod(iperiod), operiod(operiod), scale(scale), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Resample(in_sym, iperiod, operiod, scale);
    }

    void init() final
    {
        in_reg = create_reg<float>(size);
        float osize = (float)(iperiod / operiod) * size;
        out_reg = create_reg<float>(ceil(osize));

        SynthData<float> dataset(iperiod, size);
        dataset.fill(&in_reg);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*)) addr;
        query(0, iperiod * size, &out_reg, &in_reg);
    }

    void release() final
    {
        release_reg(&in_reg);
        release_reg(&out_reg);
    }

    dur_t iperiod;
    dur_t operiod;
    int64_t size;
    int64_t scale;
    region_t in_reg;
    region_t out_reg;
};

#endif  // TILT_BENCH_INCLUDE_TILT_RESAMPLE_H_
