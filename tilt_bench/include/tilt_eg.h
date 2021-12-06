#ifndef TILT_BENCH_INCLUDE_TILT_EG_H_
#define TILT_BENCH_INCLUDE_TILT_EG_H_

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _Query1(_sym in, int64_t window, int64_t win1, int64_t win2)
{
    auto win = in[_win(-window, 0)];
    auto win_sym = _sym("win", win);

    auto wsum1 = _WindowSum(win_sym, win1);
    auto wsum1_sym = _sym("wsum1", wsum1);
    auto wsum2 = _WindowSum(win_sym, win2, win1);
    auto wsum2_sym = _sym("wsum2", wsum2);

    auto w1 = _f32(win1);
    auto w1_sym = _sym("w1", w1);
    auto w2 = _f32(win2);
    auto w2_sym = _sym("w2", w2);

    auto avg1 = _Select(wsum1_sym, w1_sym, [](_sym e, _sym w) { return e / w; });
    auto avg1_sym = _sym("avg1", avg1);
    auto avg2 = _Select(wsum2_sym, w2_sym, [](_sym e, _sym w) { return e / w; });
    auto avg2_sym = _sym("avg2", avg2);

    auto join = _Join(avg1_sym, avg2_sym, [](_sym l, _sym r) { return _new(vector<Expr>{l, r}); });
    auto join_sym = _sym("join", join);

    auto zero = _f32(0);
    auto zero_sym = _sym("zero", zero);
    auto sel = _Select(join_sym, zero_sym, [](_sym e, _sym z) {
        auto avg1 = e << 0;
        auto avg2 = e << 1;
        return _cast(types::INT8, _gt(avg1, avg2));
    });
    auto sel_sym = _sym("sel", sel);

    // query operation
    auto query_op = _op(
        _iter(0, window),
        Params{ in },
        SymTable{
            {win_sym, win},
            {wsum1_sym, wsum1},
            {wsum2_sym, wsum2},
            {w1_sym, w1},
            {w2_sym, w2},
            {avg1_sym, avg1},
            {avg2_sym, avg2},
            {join_sym, join},
            {zero_sym, zero},
            {sel_sym, sel}
        },
        _true(),
        sel_sym);

    return query_op;
}

Op _Query2(_sym in, int64_t window, int64_t win1, int64_t win2)
{
    auto win = in[_win(-window, 0)];
    auto win_sym = _sym("win", win);

    auto wsum1 = _WindowSum(win_sym, win1);
    auto wsum1_sym = _sym("wsum1", wsum1);
    auto wsum2 = _WindowSum(win_sym, win2, win1);
    auto wsum2_sym = _sym("wsum2", wsum2);

    auto join = _Join(wsum1_sym, wsum2_sym, [win1, win2](_sym l, _sym r) {
        auto avg1 = l / _f32(win1);
        auto avg2 = r / _f32(win2);

        return _cast(types::INT8, _gt(avg1, avg2));
    });
    auto join_sym = _sym("join", join);

    // query operation
    auto query_op = _op(
        _iter(0, window),
        Params{ in },
        SymTable{
            {win_sym, win},
            {wsum1_sym, wsum1},
            {wsum2_sym, wsum2},
            {join_sym, join},
        },
        _true(),
        join_sym);

    return query_op;
}

Op _Query3(_sym in, int64_t window, int64_t win1, int64_t win2)
{
    auto win = in[_win(-window, 0)];
    auto win_sym = _sym("win", win);

    auto wsum1 = _WindowSum(win_sym, win1);
    auto wsum1_sym = _sym("wsum1", wsum1);
    auto wsum2 = _WindowSum(win_sym, win1);
    auto wsum2_sym = _sym("wsum2", wsum2);

    auto sel = _Select(wsum2_sym, [](_sym e) {return e;});
    auto sel_sym = _sym("sel", sel);

    auto join = _Join(wsum1_sym, sel_sym, [win1, win2](_sym l, _sym r) {
        auto avg1 = l / _f32(win1);
        auto avg2 = r / _f32(win2);

        return _cast(types::INT8, _gt(avg1, avg2));
    });
    auto join_sym = _sym("join", join);

    // query operation
    auto query_op = _op(
        _iter(0, window),
        Params{ in },
        SymTable{
            {win_sym, win},
            {wsum1_sym, wsum1},
            {wsum2_sym, wsum2},
            {sel_sym, sel},
            {join_sym, join},
        },
        _true(),
        join_sym);

    return query_op;
}

Op _WindowJoin(_sym left, _sym right, int64_t w, function<Expr(_sym, _sym)> joiner)
{
    auto lwin = left[_win(-w, 0)];
    auto lwin_sym = _sym("lwin", lwin);
    auto rwin = left[_win(-w, 0)];
    auto rwin_sym = _sym("rwin", rwin);

    auto join = _Join(lwin_sym, rwin_sym, joiner);
    auto join_sym = _sym("wjoin", join);

    return _op(
        _iter(0, w),
        Params{ left, right },
        SymTable{
            {lwin_sym, lwin},
            {rwin_sym, rwin},
            {join_sym, join}
        },
        _true(),
        join_sym);
}

Op _Query4(_sym in, int64_t window, int64_t win1, int64_t win2)
{
    auto win = in[_win(-window, 0)];
    auto win_sym = _sym("win", win);

    auto wsum1 = _WindowSum(win_sym, win1);
    auto wsum1_sym = _sym("wsum1", wsum1);
    auto wsum2 = _WindowSum(win_sym, win1);
    auto wsum2_sym = _sym("wsum2", wsum2);

    auto sel = _Select(wsum2_sym, [](_sym e) {return e;});
    auto sel_sym = _sym("sel", sel);

    auto join = _WindowJoin(wsum1_sym, sel_sym, win1, [win1, win2](_sym l, _sym r) {
        auto avg1 = l / _f32(win1);
        auto avg2 = r / _f32(win2);

        return _cast(types::INT8, _gt(avg1, avg2));
    });
    auto join_sym = _sym("join", join);

    // query operation
    auto query_op = _op(
        _iter(0, window),
        Params{ in },
        SymTable{
            {win_sym, win},
            {wsum1_sym, wsum1},
            {wsum2_sym, wsum2},
            {sel_sym, sel},
            {join_sym, join},
        },
        _true(),
        join_sym);

    return query_op;
}

Op _WindowSelect(_sym in, _sym val, int64_t w, function<Expr(_sym, _sym)> selector)
{
    auto win = in[_win(-w, 0)];
    auto win_sym = _sym("win", win);
    auto v_sym = _sym("v", val);

    auto join = _Select(win_sym, v_sym, selector);
    auto join_sym = _sym("wjoin", join);

    return _op(
        _iter(0, w),
        Params{ in, val },
        SymTable{
            {win_sym, win},
            {v_sym, val},
            {join_sym, join}
        },
        _true(),
        join_sym);
}

Op _Query5(_sym in, int64_t window, int64_t win1, int64_t win2)
{
    auto win = in[_win(-win1, 0)];
    auto win_sym = _sym("win", win);

    auto sum1 = _Sum(win_sym);
    auto sum1_sym = _sym("sum1", sum1);
    auto sum = _Sum(win_sym);
    auto sum_sym = _sym("sum", sum);
    auto sum2 = sum1_sym + sum_sym;
    auto sum2_sym = _sym("sum2", sum2);

    auto join = _WindowSelect(win_sym, sum2_sym, win1, [win1, win2](_sym l, _sym r) {
        auto avg1 = l / _f32(win1);
        auto avg2 = r / _f32(win2);

        return _cast(types::INT8, _gt(avg1, avg2));
    });
    auto join_sym = _sym("join", join);

    // query operation
    auto query_op = _op(
        _iter(0, win1),
        Params{ in },
        SymTable{
            {win_sym, win},
            {sum1_sym, sum1},
            {sum_sym, sum},
            {sum2_sym, sum2},
            {join_sym, join},
        },
        _true(),
        join_sym);

    return query_op;
}

Op _Query6(_sym in, int64_t window, int64_t win1, int64_t win2)
{
    auto win = in[_win(-win1, 0)];
    auto win_sym = _sym("win", win);

    auto sum1 = _Sum(win_sym);
    auto sum1_sym = _sym("sum1", sum1);
    auto sum = _Sum(win_sym);
    auto sum_sym = _sym("sum", sum);
    auto sum2 = sum1_sym + sum_sym;
    auto sum2_sym = _sym("sum2", sum2);
    auto avg1 = sum1_sym / _f32(win1);
    auto avg2 = sum2_sym / _f32(win2);
    auto join = _cast(types::INT8, _gt(avg1, avg2));
    auto join_sym = _sym("join", join);

    // query operation
    auto query_op = _op(
        _iter(0, win1),
        Params{ in },
        SymTable{
            {win_sym, win},
            {sum1_sym, sum1},
            {sum_sym, sum},
            {sum2_sym, sum2},
            {join_sym, join},
        },
        _true(),
        join_sym);

    return query_op;
}

Op _Query7(_sym in, int64_t window, int64_t win1, int64_t win2)
{
    auto win = in[_win(-win1, 0)];
    auto win_sym = _sym("win", win);

    auto sum1 = _Sum(win_sym);
    auto sum1_sym = _sym("sum1", sum1);
    auto sum2 = sum1_sym + sum1_sym;
    auto sum2_sym = _sym("sum2", sum2);
    auto avg1 = sum1_sym / _f32(win1);
    auto avg2 = sum2_sym / _f32(win2);
    auto join = _cast(types::INT8, _gt(avg1, avg2));
    auto join_sym = _sym("join", join);

    // query operation
    auto query_op = _op(
        _iter(0, win1),
        Params{ in },
        SymTable{
            {win_sym, win},
            {sum1_sym, sum1},
            {sum2_sym, sum2},
            {join_sym, join},
        },
        _true(),
        join_sym);

    return query_op;
}
class EgBench : public Benchmark {
public:
    EgBench(dur_t period, int64_t window, int64_t win1, int64_t win2, int64_t size) :
        period(period), window(window), win1(win1), win2(win2), size(size)
    {}

protected:
    void init() final
    {
        in_reg = create_reg<float>(size);
        out_reg = create_reg<bool>(size);

        SynthData<float> dataset(period, size);
        dataset.fill(&in_reg);
    }

    void release() final
    {
        release_reg(&in_reg);
        release_reg(&out_reg);
    }

    int64_t window;
    int64_t win1;
    int64_t win2;
    dur_t period;
    int64_t size;
    region_t in_reg;
    region_t state_reg;
    region_t out_reg;
};

class Eg1Bench : public EgBench {
public:
    Eg1Bench(dur_t period, int64_t window, int64_t win1, int64_t win2, int64_t size) :
        EgBench(period, window, win1, win2, size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Query1(in_sym, window, win1, win2);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*)) addr;
        query(0, period * size, &out_reg, &in_reg);
    }
};

class Eg2Bench : public EgBench {
public:
    Eg2Bench(dur_t period, int64_t window, int64_t win1, int64_t win2, int64_t size) :
        EgBench(period, window, win1, win2, size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Query2(in_sym, window, win1, win2);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*)) addr;
        query(0, period * size, &out_reg, &in_reg);
    }
};

class Eg3Bench : public EgBench {
public:
    Eg3Bench(dur_t period, int64_t window, int64_t win1, int64_t win2, int64_t size) :
        EgBench(period, window, win1, win2, size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Query3(in_sym, window, win1, win2);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*)) addr;
        query(0, period * size, &out_reg, &in_reg);
    }
};

class Eg4Bench : public EgBench {
public:
    Eg4Bench(dur_t period, int64_t window, int64_t win1, int64_t win2, int64_t size) :
        EgBench(period, window, win1, win2, size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Query4(in_sym, window, win1, win2);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*)) addr;
        query(0, period * size, &out_reg, &in_reg);
    }
};

class Eg5Bench : public EgBench {
public:
    Eg5Bench(dur_t period, int64_t window, int64_t win1, int64_t win2, int64_t size) :
        EgBench(period, window, win1, win2, size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Query5(in_sym, window, win1, win2);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*)) addr;
        query(0, period * size, &out_reg, &in_reg);
    }
};

class Eg6Bench : public EgBench {
public:
    Eg6Bench(dur_t period, int64_t window, int64_t win1, int64_t win2, int64_t size) :
        EgBench(period, window, win1, win2, size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Query6(in_sym, window, win1, win2);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*)) addr;
        query(0, period * size, &out_reg, &in_reg);
    }
};

class Eg7Bench : public EgBench {
public:
    Eg7Bench(dur_t period, int64_t window, int64_t win1, int64_t win2, int64_t size) :
        EgBench(period, window, win1, win2, size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Query7(in_sym, window, win1, win2);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*)) addr;
        query(0, period * size, &out_reg, &in_reg);
    }
};

#endif  // TILT_BENCH_INCLUDE_TILT_EG_H_
