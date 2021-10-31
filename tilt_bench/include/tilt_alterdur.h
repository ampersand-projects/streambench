#ifndef TILT_BENCH_INCLUDE_TILT_ALTERDUR_H_
#define TILT_BENCH_INCLUDE_TILT_ALTERDUR_H_

#include "tilt/builder/tilder.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _AlterDur(_sym in, int64_t iperiod, int64_t operiod)
{
    auto e = in[_pt(0)];
    auto e_sym = _sym("e", e);
    auto alt_op = _op(
        _iter(0, operiod),
        Params{in},
        SymTable{{e_sym, e}},
        _true(),
        e_sym);
    return alt_op;
}

class AlterDurBench : public Benchmark {
public:
    AlterDurBench(dur_t iperiod, dur_t operiod, int64_t size) :
        iperiod(iperiod), operiod(operiod), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _AlterDur(in_sym, iperiod, operiod);
    }

    void init() final
    {
        in_reg = create_reg<float>(size);
        out_reg = create_reg<float>(size);

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

    region_t in_reg;
    region_t out_reg;

    int64_t size;
    dur_t iperiod;
    dur_t operiod;
};

#endif  // TILT_BENCH_INCLUDE_TILT_SELECT_H_
