#ifndef TILT_BENCH_INCLUDE_TILT_KURT_H_
#define TILT_BENCH_INCLUDE_TILT_KURT_H_

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _Kurt(_sym in, int64_t window)
{
    auto inwin = in[_win(-window, 0)];
    auto inwin_sym = _sym("inwin", inwin);

    auto rms_acc = [](Expr s, Expr st, Expr et, Expr d) {
        auto max = _get(s, 0);
        auto sum = _get(s, 1);
        auto count = _get(s, 2);
        auto sq = _get(s, 3);
        return _new(vector<Expr>{_max(max, d), _add(sum, d), _add(count, _f32(1)), _add(sq, _mul(d, d))});
    };
    auto rms_state = _red(inwin_sym, _new(vector<Expr>{_f32(0), _f32(0), _f32(0), _f32(0)}), rms_acc);
    auto rms_state_sym = _sym("rms_state", rms_state);

    auto rms_op = _Select(inwin_sym, rms_state_sym, [](_sym e, _sym rms_state) {
                        auto max = _get(rms_state, 0);
                        auto sum = _get(rms_state, 1);
                        auto count = _get(rms_state, 2);
                        auto sq = _get(rms_state, 3);
                        
                        auto avg = sum / count;
                        auto rms = _sqrt(sq / count);
                        auto cf = max / rms;
                        return _new(vector<Expr>{e, e - avg, rms, cf});
                    });
    auto rms_op_sym = _sym("rmsop", rms_op);

    auto kurt_acc = [](Expr s, Expr st, Expr et, Expr d) {
        auto avg = _get(d, 1);
        return _add(s, (avg * avg * avg * avg));
    };
    auto kurt_state = _red(rms_op_sym, _f32(0), kurt_acc);
    auto kurt_state_sym = _sym("kurt_state", kurt_state);

    auto kurt_op = _Select(rms_op_sym, kurt_state_sym, [](_sym e, _sym kurt_state) {
                        auto rms = e << 2;
                        auto cf = e << 3;
                        auto pow4 = rms * rms * rms * rms;
                        return _new(vector<Expr>{kurt_state/pow4, rms, cf, _f32(0)});
                    });
    auto kurt_op_sym = _sym("kurtop", kurt_op);

    // query operation
    auto query_op = _op(
        _iter(0, window),
        Params{ in },
        SymTable{
            {inwin_sym, inwin},
            {rms_state_sym, rms_state},
            {rms_op_sym, rms_op},
            {kurt_state_sym, kurt_state},
            {kurt_op_sym, kurt_op}
        },
        _true(),
        kurt_op_sym);

    return query_op;
}

struct KurtState {
    float kurt;
    float rms;
    float cf;
    float dummy;
};

class KurtBench : public Benchmark {
public:
    KurtBench(dur_t period, int64_t window, int64_t size) :
        period(period), window(window), size(size)
    {}

    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Kurt(in_sym, window);
    }

    void init() final
    {
        in_reg = create_reg<float>(size);
        out_reg = create_reg<KurtState>(size);

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

#endif  // TILT_BENCH_INCLUDE_TILT_KURT_H_
