#ifndef TILT_BENCH_INCLUDE_TILT_QTY_H_
#define TILT_BENCH_INCLUDE_TILT_QTY_H_

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _LargeQty_Slow(_sym in, int64_t p, int64_t window)
{
    auto win = in[_win(-window, 0)];
    auto win_sym = _sym("win", win);

    auto acc = [](Expr s, Expr st, Expr et, Expr d) {
        auto sum = _get(s, 0);
        auto square = _get(s, 1);
        auto count = _get(s, 2);
        return _new(vector<Expr>{_add(sum, d), _add(square, _mul(d, d)), _add(count, _f32(1)), _f32(0)});
    };
    auto zscore = _red(win_sym, _new(vector<Expr>{_f32(0), _f32(0), _f32(0), _f32(0)}), acc);
    auto zscore_sym = _sym("zscore", zscore);

    auto sum = zscore_sym << 0;
    auto square = zscore_sym << 1;
    auto count = zscore_sym << 2;

    auto avg = sum / count;
    auto avg_sym = _sym("avg", avg);
    auto stddev = (square / count) - (avg_sym * avg_sym);
    auto stddev_sym = _sym("stddev", stddev);

    auto res = _new(vector<Expr>{avg_sym, stddev_sym});
    auto res_sym = _sym("res", res);

    return _op(
        _iter(0, p),
        Params{in},
        SymTable{
            {win_sym, win},
            {zscore_sym, zscore},
            {avg_sym, avg},
            {stddev_sym, stddev},
            {res_sym, res}
        },
        _true(),
        res_sym);
}

Op _MovingZScore(_sym in, int64_t p, int64_t window)
{
    auto out = _out(types::STRUCT<float, float, float, float>());

    auto head = in[_pt(0)];
    auto head_sym = _sym("head", head);
    auto tail = in[_pt(-window)];
    auto tail_sym = _sym("tail", tail);

    auto o = out[_pt(-p)];
    auto o_sym = _sym("o", o);
    auto state = _ifelse(_exists(o_sym), o_sym, _new(vector<Expr>{_f32(0), _f32(0), _f32(0), _f32(0)}));
    auto state_sym = _sym("state", state);

    auto val = state_sym << 0;
    auto sum = state_sym << 1;
    auto square = state_sym << 2;
    auto count = state_sym << 3;

    auto new_sum = sum
        + _ifelse(_exists(head_sym), head_sym, _f32(0))
        - _ifelse(_exists(tail_sym), tail_sym, _f32(0));
    auto new_square = square
        + _ifelse(_exists(head_sym), head_sym * head_sym, _f32(0))
        - _ifelse(_exists(tail_sym), tail_sym * tail_sym, _f32(0));
    auto new_count = count
        + _ifelse(_exists(head_sym), _f32(1), _f32(0))
        - _ifelse(_exists(tail_sym), _f32(1), _f32(0));

    auto res = _new(vector<Expr>{head_sym, new_sum, new_square, new_count});
    auto res_sym = _sym("res", res);

    return _op(
        _iter(0, p),
        Params{in},
        SymTable{
            {head_sym, head},
            {tail_sym, tail},
            {o_sym, o},
            {state_sym, state},
            {res_sym, res}
        },
        _true(),
        res_sym);
}

Op _LargeQty(_sym in, int64_t p, int64_t window, int64_t scale)
{
    auto state = _sym("state", tilt::Type(types::STRUCT<float, float, float, float>(), _iter(0, -1)));

    auto win = in[_win(-p * scale - window, 0)];
    auto win_sym = _sym("win", win);

    auto mz = _MovingZScore(win_sym, p, window);
    auto mz_sym = _sym("mz", mz);

    auto val = _f32(0);
    auto val_sym = _sym("val", val);

    auto sel = _Select(mz_sym, val_sym, [](_sym e, _sym val) {
        auto value = e << 0;
        auto sum = e << 1;
        auto square = e << 2;
        auto count = e << 3;
        auto avg = (sum / count);
        auto var = (square / count) - (avg * avg);
        auto stddev = _sqrt(var);
        return _new(vector<Expr>{value, avg, stddev, count});
    });
    auto sel_sym = _sym("sel", sel);

    auto maco_op = _op(
        _iter(0, p * scale),
        Params{in, state},
        SymTable{
            {win_sym, win},
            {mz_sym, mz},
            {val_sym, val},
            {sel_sym, sel}
        },
        _true(),
        sel_sym,
        Aux{
            {mz_sym, state}
        });

    return maco_op;
}

struct ZScore {
    float val;
    float sum;
    float square;
    float count;

    string str() const
    {
        return "{"
            + to_string(val) + ", "
            + to_string(sum) + ", "
            + to_string(square) + ", "
            + to_string(count) + "}";
    }
};

struct StdDev {
    float val;
    float avg;
    float stddev;
    float count;

    string str() const
    {
        return "{"
            + to_string(val) + ", "
            + to_string(avg) + ", "
            + to_string(stddev) + ", "
            + to_string(count) + "}";
    }
};

class LargeQtyBench : public Benchmark {
public:
    LargeQtyBench(int64_t period, int64_t window, int64_t scale, int64_t size) :
        period(period), window(window), scale(scale), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _LargeQty(in_sym, period, window, scale);
    }

    void init() final
    {
        in_reg = create_reg<float>(size);
        state_reg = create_reg<ZScore>(scale);
        out_reg = create_reg<StdDev>(size);

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
        release_reg(&out_reg);
    }

    dur_t period;
    int64_t window;
    int64_t scale;
    int64_t size;
    region_t in_reg;
    region_t state_reg;
    region_t out_reg;
};

#endif  // TILT_BENCH_INCLUDE_TILT_QTY_H_
