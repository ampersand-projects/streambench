#ifndef TILT_BENCH_INCLUDE_TILT_WHERE_H_
#define TILT_BENCH_INCLUDE_TILT_WHERE_H_

#include "tilt/builder/tilder.h"
#include "tilt_bench.h"
#include "tilt_base.h"

#include "iostream"

using namespace tilt;
using namespace tilt::tilder;

class WhereBench : public Benchmark {
public:
    WhereBench(dur_t period, int64_t size) :
        period(period), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Where(in_sym, [](_sym in) { return _gt(in, _f32(0)); });
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

#endif  // TILT_BENCH_INCLUDE_TILT_WHERE_H_
