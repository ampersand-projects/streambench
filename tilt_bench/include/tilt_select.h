#ifndef TILT_BENCH_INCLUDE_TILT_SELECT_H_
#define TILT_BENCH_INCLUDE_TILT_SELECT_H_

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _Select(_sym in, function<Expr(_sym)> selector)
{
    auto e = in[_pt(0)];
    auto e_sym = _sym("e", e);
    auto res = selector(e_sym);
    auto res_sym = _sym("res", res);
    auto sel_op = _op(
        _iter(0, 1),
        Params{in},
        SymTable{{e_sym, e}, {res_sym, res}},
        _exists(e_sym),
        res_sym);
    return sel_op;
}

class SelectBench : public Benchmark {
public:
    SelectBench(dur_t period, int64_t size) :
        period(period), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Select(in_sym, [](_sym in) { return in + _f32(10); });
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

    region_t in_reg;
    region_t out_reg;

    int64_t size;
    dur_t period;
};

#endif  // TILT_BENCH_INCLUDE_TILT_SELECT_H_
