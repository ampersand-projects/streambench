#ifndef TILT_BENCH_INCLUDE_TILT_AGGREGATE_H_
#define TILT_BENCH_INCLUDE_TILT_AGGREGATE_H_

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

class AggregateBench : public Benchmark {
public:
    AggregateBench(dur_t period, int64_t size, int64_t w) :
        period(period), size(size), w(w)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _WindowSum(in_sym, w);
    }

    void init() final
    {
        in_reg = create_reg<float>(size);
        float osize = (float)size / (float)w;
        out_reg = create_reg<float>(ceil(osize));

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
    int64_t w;
};

#endif  // TILT_BENCH_INCLUDE_TILT_AGGREGATE_H_
