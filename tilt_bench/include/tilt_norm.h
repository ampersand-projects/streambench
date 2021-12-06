#ifndef TILT_BENCH_INCLUDE_TILT_NORM_H_
#define TILT_BENCH_INCLUDE_TILT_NORM_H_

#include <thread>

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
    NormBench(dur_t period, int64_t window, int64_t size, int par) :
        period(period), window(window), size(size), par(par)
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
        SynthData<float> dataset(period, size);
        dataset.fill(&in_reg);

        for (int i=0; i<par; i++) {
            out_regs.push_back(create_reg<float>(1000));
        }
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*)) addr;

        std::vector<thread> split;
        for (int i=0; i<par; i++) {
            split.push_back(thread([=] () {
                query(0, period * size, &out_regs[i], &in_reg);
            }));
        }
        for (int i=0; i<par; i++) {
            split[i].join();
        }
    }

    void release() final
    {
        release_reg(&in_reg);
        for (int i=0; i<par; i++) {
            release_reg(&out_regs[i]);
        }
    }

    int64_t window;
    dur_t period;
    int64_t size;
    int par;
    region_t in_reg;
    vector<region_t> out_regs;
};

#endif  // TILT_BENCH_INCLUDE_TILT_NORM_H_
