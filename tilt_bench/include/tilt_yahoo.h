#ifndef TILT_BENCH_INCLUDE_TILT_YAHOO_H_
#define TILT_BENCH_INCLUDE_TILT_YAHOO_H_

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _Yahoo(_sym in, int64_t w)
{
    auto window = in[_win(-w, 0)];
    auto window_sym = _sym("win", window);

    auto acc = [](Expr s, Expr st, Expr et, Expr d) {
        auto e_type = _get(d, 2);
        auto flag = _sel(_eq(e_type, _i64(1)), _i32(1), _i32(0));
        return _add(s, flag);
    };
    auto count = _red(window_sym, _i32(0), acc);
    auto count_sym = _sym("count", count);
    auto wc_op = _op(
        _iter(0, w),
        Params{ in },
        SymTable{ {window_sym, window}, {count_sym, count} },
        _true(),
        count_sym);
    return wc_op;
}

class YahooBench : public Benchmark {
public:
    YahooBench(dur_t period, int64_t size, int64_t w) :
        period(period), size(size), w(w)
    {}

    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::STRUCT<long, long, long, long>(), _iter(0, -1)));
        return _Yahoo(in_sym, w);
    }

    void init() final
    {
        in_reg = create_reg<Yahoo>(size);
        float osize = (float)size / (float)w;
        out_reg = create_reg<int>(ceil(osize));

        YahooData dataset(period, size);
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
