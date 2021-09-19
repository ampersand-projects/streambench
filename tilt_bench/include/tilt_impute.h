#ifndef TILT_BENCH_INCLUDE_TILT_IMPUTE_H_
#define TILT_BENCH_INCLUDE_TILT_IMPUTE_H_

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _FillMean(_sym in)
{
    // avg state
    auto avg_state = _Average(in, [](Expr e) { return e; });
    auto avg_state_sym = _sym("avg_state", avg_state);

    // avg value
    auto avg = _div(_get(avg_state_sym, 0), _get(avg_state_sym, 1));
    auto avg_sym = _sym("avg", avg);

    auto e = in[_pt(0)];
    auto e_sym = _sym("e", e);
    auto res = _ifelse(_exists(e_sym), e_sym, avg_sym);
    auto res_sym = _sym("res", res);
    auto sel_op = _op(
        _iter(0, 1),
        Params{in},
        SymTable{
            {e_sym, e},
            {avg_state_sym, avg_state},
            {avg_sym, avg},
            {res_sym, res}
        },
        _true(),
        res_sym);
    return sel_op;
}

Op _Impute(_sym in, int64_t window)
{
    auto win = in[_win(-window, 0)];
    auto win_sym = _sym("win", win);

    auto fm = _FillMean(win_sym);
    auto fm_sym = _sym("fm", fm);

    return _op(
        _iter(0, window),
        Params{in},
        SymTable{
            {win_sym, win},
            {fm_sym, fm}
        },
        _exists(win_sym),
        fm_sym);
}

class ImputeBench : public Benchmark {
public:
    ImputeBench(dur_t period, int64_t window, int64_t size) :
        period(period), window(window), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Impute(in_sym, window);
    }

    void init() final
    {
        in_reg = create_reg<float>(size);
        out_reg = create_reg<float>(size);

        SynthData<float> dataset(period, size);
        dataset.fill(&in_reg);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*)) addr;
        query(0, period * size, &out_reg, &in_reg);
    }

    void release() final
    {
        release_reg(&in_reg);
        release_reg(&out_reg);
    }

    int64_t window;
    dur_t period;
    int64_t size;
    region_t in_reg;
    region_t out_reg;
};

#endif  // TILT_BENCH_INCLUDE_TILT_IMPUTE_H_
