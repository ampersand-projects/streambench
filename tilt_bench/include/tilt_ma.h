#ifndef TILT_BENCH_INCLUDE_TILT_MA_H_
#define TILT_BENCH_INCLUDE_TILT_MA_H_

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _MovingAverage(_sym in, int64_t p, int64_t w_short, int64_t w_long)
{
    auto out = _out(types::STRUCT<float, float, float, float>());

    auto head = in[_pt(0)];
    auto head_sym = _sym("head", head);
    auto t_short = in[_pt(-w_short)];
    auto t_short_sym = _sym("t_short", t_short);
    auto t_long = in[_pt(-w_long)];
    auto t_long_sym = _sym("t_long", t_long);
    auto o = out[_pt(-p)];
    auto o_sym = _sym("o", o);
    auto state = _ifelse(_exists(o_sym), o_sym, _new(vector<Expr>{_f32(0), _f32(0), _f32(0), _f32(0)}));
    auto state_sym = _sym("state", state);

    auto short_sum = state_sym << 0;
    auto short_count = state_sym << 1;
    auto long_sum = state_sym << 2;
    auto long_count = state_sym << 3;

    auto new_short_sum = short_sum
        + _ifelse(_exists(head_sym), head_sym, _f32(0))
        - _ifelse(_exists(t_short_sym), t_short_sym, _f32(0));
    auto new_long_sum = long_sum
        + _ifelse(_exists(head_sym), head_sym, _f32(0))
        - _ifelse(_exists(t_long_sym), t_long_sym, _f32(0));
    auto new_short_count = short_count
        + _ifelse(_exists(head_sym), _f32(1), _f32(0))
        - _ifelse(_exists(t_short_sym), _f32(1), _f32(0));
    auto new_long_count = long_count
        + _ifelse(_exists(head_sym), _f32(1), _f32(0))
        - _ifelse(_exists(t_long_sym), _f32(1), _f32(0));

    auto res = _new(vector<Expr>{new_short_sum, new_short_count, new_long_sum, new_long_count});
    auto res_sym = _sym("res", res);

    auto ma_op = _op(
        _iter(0, p),
        Params{in},
        SymTable{
            {head_sym, head},
            {t_short_sym, t_short},
            {t_long_sym, t_long},
            {o_sym, o},
            {state_sym, state},
            {res_sym, res}
        },
        _true(),
        res_sym);

    return ma_op;
}

Op _MACrossOver(_sym in, int64_t p, int64_t w_short, int64_t w_long, int64_t scale)
{
    auto state = _sym("state", tilt::Type(types::STRUCT<float, float, float, float>(), _iter(0, -1)));

    auto long_win = in[_win(-p * scale - w_long, 0)];
    auto long_win_sym = _sym("long_win", long_win);

    auto ma = _MovingAverage(long_win_sym, p, w_short, w_long);
    auto ma_sym = _sym("ma", ma);

    auto val = _f32(0);
    auto val_sym = _sym("val", val);

    auto sel = _Select(ma_sym, val_sym, [](_sym e, _sym val) {
        auto short_sum = e << 0;
        auto short_count = e << 1;
        auto long_sum = e << 2;
        auto long_count = e << 3;
        auto short_avg = (short_sum / short_count);
        auto long_avg = (long_sum / long_count);
        return _cast(types::INT8, _gt(short_avg, long_avg));
    });
    auto sel_sym = _sym("sel", sel);

    auto maco_op = _op(
        _iter(0, p * scale),
        Params{in, state},
        SymTable{
            {long_win_sym, long_win},
            {ma_sym, ma},
            {val_sym, val},
            {sel_sym, sel}
        },
        _true(),
        sel_sym,
        Aux{
            {ma_sym, state}
        });

    return maco_op;
}

struct MOCAState {
    float short_sum;
    float short_count;
    float long_sum;
    float long_count;
};

class MOCABench : public Benchmark {
public:
    MOCABench(int64_t period, int64_t w_short, int64_t w_long, int64_t scale, int64_t size) :
        period(period), w_short(w_short), w_long(w_long), scale(scale), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _MACrossOver(in_sym, period, w_short, w_long, scale);
    }

    void init() final
    {
        in_reg = create_reg<float>(size);
        state_reg = create_reg<MOCAState>(size);
        out_reg = create_reg<bool>(size);

        SynthData<float> dataset(period, size);
        dataset.fill(&in_reg);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*, region_t*)) addr;
        query(0, period * size, &out_reg, &in_reg, &state_reg);
    }

    void release() final
    {
        release_reg(&in_reg);
        release_reg(&state_reg);
        release_reg(&out_reg);
    }

    dur_t period;
    int64_t w_short;
    int64_t w_long;
    int64_t scale;
    int64_t size;
    region_t in_reg;
    region_t state_reg;
    region_t out_reg;
};

#endif  // TILT_BENCH_INCLUDE_TILT_MA_H_
