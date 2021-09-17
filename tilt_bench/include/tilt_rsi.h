#ifndef TILT_BENCH_INCLUDE_TILT_RSI_H_
#define TILT_BENCH_INCLUDE_TILT_RSI_H_

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _DailyDiff(_sym in, int64_t p)
{
    auto cur = in[_pt(0)];
    auto cur_sym = _sym("cur", cur);
    auto prev = in[_pt(-p)];
    auto prev_sym = _sym("prev", prev);
    auto res = _ifelse(_exists(prev_sym), prev_sym - cur_sym, _f32(0));
    auto res_sym = _sym("res", res);

    auto diff_op = _op(
        _iter(0, p),
        Params{in},
        SymTable{
            {cur_sym, cur},
            {prev_sym, prev},
            {res_sym, res}
        },
        _exists(cur_sym),
        res_sym);

    return diff_op;
}

Op _RSI(_sym in, int64_t p, int64_t window)
{
    auto out = _out(types::STRUCT<float, float>());

    auto head = in[_pt(0)];
    auto head_sym = _sym("head", head);
    auto tail = in[_pt(-window)];
    auto tail_sym = _sym("t_short", tail);
    auto o = out[_pt(-p)];
    auto o_sym = _sym("o", o);
    auto state = _ifelse(_exists(o_sym), o_sym, _new(vector<Expr>{_f32(0), _f32(0)}));
    auto state_sym = _sym("state", state);

    auto pos_sum = state_sym << 0;
    auto neg_sum = state_sym << 1;

    auto new_pos_sum = pos_sum
        + _ifelse(_exists(head_sym) && _gt(head_sym, _f32(0)), head_sym, _f32(0))
        - _ifelse(_exists(tail_sym) && _gt(tail_sym, _f32(0)), tail_sym, _f32(0));
    auto new_neg_sum = neg_sum
        + _ifelse(_exists(head_sym) && _lt(head_sym, _f32(0)), -head_sym, _f32(0))
        - _ifelse(_exists(tail_sym) && _lt(tail_sym, _f32(0)), -tail_sym, _f32(0));

    auto res = _new(vector<Expr>{new_pos_sum, new_neg_sum});
    auto res_sym = _sym("res", res);

    auto rsi_op = _op(
        _iter(0, p),
        Params{in},
        SymTable{
            {head_sym, head},
            {tail_sym, tail},
            {o_sym, o},
            {state_sym, state},
            {res_sym, res}
        },
        _true(),
        res_sym);

    return rsi_op;
}

Op _RSICalc(_sym in, int64_t p, int64_t window, int64_t scale)
{
    auto state = _sym("state", tilt::Type(types::STRUCT<float, float>(), _iter(0, -1)));

    auto win = in[_win(-p * (scale + 1), 0)];
    auto win_sym = _sym("win", win);

    auto dailydiff = _DailyDiff(win_sym, p);
    auto dailydiff_sym = _sym("dailydiff", dailydiff);

    auto rsi = _RSI(dailydiff_sym, p, window);
    auto rsi_sym = _sym("rsi", rsi);

    auto val = _f32(0);
    auto val_sym = _sym("val", val);

    auto sel = _Select(rsi_sym, val_sym, [](_sym e, _sym val) {
        auto pos_sum = e << 0;
        auto neg_sum = e << 1;
        return _f32(100) - (_f32(100) / (_f32(1) + (pos_sum / neg_sum)));
    });
    auto sel_sym = _sym("sel", sel);

    return _op(
        _iter(0, p * scale),
        Params{in, state},
        SymTable{
            {win_sym, win},
            {dailydiff_sym, dailydiff},
            {rsi_sym, rsi},
            {val_sym, val},
            {sel_sym, sel}
        },
        _true(),
        sel_sym,
        Aux{
            {rsi_sym, state}
        });
}

struct RSIState {
    float pos_sum;
    float neg_sum;

    string str() const
    {
        return "{" + to_string(pos_sum) + ", " + to_string(neg_sum) + "}";
    }
};

class RSIBench : public Benchmark {
public:
    RSIBench(int64_t period, int64_t window, int64_t scale, int64_t size) :
        period(period), window(window), scale(scale), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _RSICalc(in_sym, period, window, scale);
    }

    void init() final
    {
        in_reg = create_reg<float>(size);
        state_reg = create_reg<RSIState>(scale);
        out_reg = create_reg<float>(size);

        SynthData<float> dataset(period, size);
        dataset.fill(&in_reg);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*, region_t*)) addr;
        query(0, period * size, &out_reg, &in_reg, &state_reg);
    }

    void release() final
    {
        auto size = 100;
        for (int i = 0; i < size; i++) {
            cout << "(" << in_reg.tl[i].t << "," << in_reg.tl[i].t + in_reg.tl[i].d << ") "
                << reinterpret_cast<float*>(in_reg.data)[i] << " -> "
                << "(" << out_reg.tl[i].t << "," << out_reg.tl[i].t + out_reg.tl[i].d << ")"
                << reinterpret_cast<float*>(out_reg.data)[i] << endl;
        }
        release_reg(&in_reg);
        release_reg(&state_reg);
        release_reg(&out_reg);
    }

    dur_t period;
    int64_t window;
    int64_t scale;
    int64_t size;
    region_t in_reg;
    region_t state_reg;
    region_t out_reg;
};

#endif  // TILT_BENCH_INCLUDE_TILT_RSI_H_
