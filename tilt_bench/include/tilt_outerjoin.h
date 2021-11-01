#ifndef TILT_BENCH_INCLUDE_TILT_OUTERJOIN_H_
#define TILT_BENCH_INCLUDE_TILT_OUTERJOIN_H_

#include <numeric>

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

class OuterJoinBench : public Benchmark {
public:
    OuterJoinBench(dur_t lperiod, dur_t rperiod, int64_t size) :
        lperiod(lperiod), rperiod(rperiod), size(size)
    {}

private:
    Op query() final
    {
        auto left_sym = _sym("left", tilt::Type(types::FLOAT32, _iter(0, -1)));
        auto right_sym = _sym("right", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _OuterJoin(left_sym, right_sym);
    }

    void init() final
    {
        left_reg = create_reg<float>(size);
        right_reg = create_reg<float>(size);
        float ratio = (float) max(lperiod, rperiod) / (float) min(lperiod, rperiod);
        int osize = size * ceil(ratio);
        out_reg = create_reg<float>(osize);

        SynthData<float> dataset_left(lperiod, size);
        dataset_left.fill(&left_reg);
        SynthData<float> dataset_right(rperiod, size);
        dataset_right.fill(&right_reg);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*, region_t*)) addr;
        query(0, max(lperiod, rperiod) * size, &out_reg, &left_reg, &right_reg);
    }

    void release() final
    {
        release_reg(&left_reg);
        release_reg(&right_reg);
        release_reg(&out_reg);
    }

    region_t left_reg;
    region_t right_reg;
    region_t out_reg;

    int64_t size;
    dur_t lperiod;
    dur_t rperiod;
};

#endif  // TILT_BENCH_INCLUDE_TILT_OUTERJOIN_H_
