#ifndef TILT_BENCH_INCLUDE_TILT_NORM_H_
#define TILT_BENCH_INCLUDE_TILT_NORM_H_

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _Norm(_sym in, int64_t window)
{
    auto inwin = in[_win(-window, 0)];
    auto inwin_sym = _sym("inwin", inwin);

    // avg state
    auto avg_state = _Average(inwin_sym, [](Expr e) { return e; });
    auto avg_state_sym = _sym("avg_state", avg_state);

    // avg value
    auto avg = _div(_get(avg_state_sym, 0), _get(avg_state_sym, 1));
    auto avg_sym = _sym("avg", avg);

    // avg join
    auto avg_op = _Select(inwin_sym, avg_sym, [](_sym e, _sym avg) { return e - avg; });
    auto avg_op_sym = _sym("avgop", avg_op);

    // stddev state
    auto std_state = _Average(avg_op_sym, [](Expr e) { return _mul(e, e); });
    auto std_state_sym = _sym("stddev_state", std_state);

    // stddev value
    auto std = _sqrt(_div(_get(std_state_sym, 0), _get(std_state_sym, 1)));
    auto std_sym = _sym("std", std);

    // std join
    auto std_op = _Select(avg_op_sym, std_sym, [](_sym e, _sym std) { return e / std; });
    auto std_op_sym = _sym("stdop", std_op);

    // query operation
    auto query_op = _op(
        _iter(0, window),
        Params{ in },
        SymTable{
            {inwin_sym, inwin},
            {avg_state_sym, avg_state},
            {avg_sym, avg},
            {avg_op_sym, avg_op},
            {std_state_sym, std_state},
            {std_sym, std},
            {std_op_sym, std_op}
        },
        _true(),
        std_op_sym);

    return query_op;
}

class NormBench : public Benchmark {
public:
    NormBench(dur_t period, int64_t window, int64_t size) :
        period(period), window(window), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Norm(in_sym, window);
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

class ParallelNormBench : public ParallelBenchmark {
public:
    ParallelNormBench(int threads, dur_t period, int64_t window, int64_t size)
    {
        for (int i = 0; i < threads; i++) {
            benchs.push_back(new NormBench(period, window, size));
        }
    }
};

#endif  // TILT_BENCH_INCLUDE_TILT_NORM_H_
