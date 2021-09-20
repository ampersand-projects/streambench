#ifndef TILT_BENCH_INCLUDE_TILT_PEAK_H_
#define TILT_BENCH_INCLUDE_TILT_PEAK_H_

#include "tilt/builder/tilder.h"
#include "tilt_base.h"
#include "tilt_bench.h"

using namespace tilt;
using namespace tilt::tilder;

Op _LowPass(_sym in, int64_t p)
{
    auto out = _out(types::FLOAT32);

    auto e0 = in[_pt(0)];
    auto e0_sym = _sym("e0", e0);
    auto e6 = in[_pt(-6 * p)];
    auto e6_sym = _sym("e6", e6);
    auto e12 = in[_pt(-12 * p)];
    auto e12_sym = _sym("e12", e12);

    auto o1 = out[_pt(-p)];
    auto o1_sym = _sym("o1", o1);
    auto o2 = out[_pt(-2 * p)];
    auto o2_sym = _sym("o2", o2);

    auto e0_val = _ifelse(_exists(e0_sym), e0_sym, _f32(0));
    auto e6_val = _ifelse(_exists(e6_sym), e6_sym, _f32(0));
    auto e12_val = _ifelse(_exists(e12_sym), e12_sym, _f32(0));
    auto o1_val = _ifelse(_exists(o1_sym), o1_sym, _f32(0));
    auto o2_val = _ifelse(_exists(o2_sym), o2_sym, _f32(0));

    auto res = (_f32(2) * o1_val) - (o2_val) + e0_val - (_f32(2) * e6_val) + e12_val;
    auto res_sym = _sym("res", res);

    return _op(
        _iter(0, p),
        Params{in},
        SymTable{
            {e0_sym, e0},
            {e6_sym, e6},
            {e12_sym, e12},
            {o1_sym, o1},
            {o2_sym, o2},
            {res_sym, res}
        },
        _true(),
        res_sym);
}

Op _HighPass(_sym in, int64_t p)
{
    auto out = _out(types::FLOAT32);

    auto e0 = in[_pt(0)];
    auto e0_sym = _sym("e0", e0);
    auto e16 = in[_pt(-16 * p)];
    auto e16_sym = _sym("e16", e16);
    auto e32 = in[_pt(-32 * p)];
    auto e32_sym = _sym("e32", e32);

    auto o1 = out[_pt(-p)];
    auto o1_sym = _sym("o1", o1);

    auto e0_val = _ifelse(_exists(e0_sym), e0_sym, _f32(0));
    auto e16_val = _ifelse(_exists(e16_sym), e16_sym, _f32(0));
    auto e32_val = _ifelse(_exists(e32_sym), e32_sym, _f32(0));
    auto o1_val = _ifelse(_exists(o1_sym), o1_sym, _f32(0));

    auto res = (_f32(32) * e16_val) - o1_val + e0_val - e32_val;
    auto res_sym = _sym("res", res);

    return _op(
        _iter(0, p),
        Params{in},
        SymTable{
            {e0_sym, e0},
            {e16_sym, e16},
            {e32_sym, e32},
            {o1_sym, o1},
            {res_sym, res}
        },
        _true(),
        res_sym);
}

Op _Derive(_sym in, int64_t p)
{
    auto beat = _beat(_iter(0, p));

    auto e0 = in[_pt(0)];
    auto e0_sym = _sym("e0", e0);
    auto e1 = in[_pt(-p)];
    auto e1_sym = _sym("e1", e1);
    auto e3 = in[_pt(-3 * p)];
    auto e3_sym = _sym("e3", e3);
    auto e4 = in[_pt(-4 * p)];
    auto e4_sym = _sym("e4", e4);

    auto t = _cast(types::FLOAT32, beat[_pt(-2 * p)]);
    auto t_sym = _sym("t", t);

    auto e0_val = _ifelse(_exists(e0_sym), e0_sym, _f32(0));
    auto e1_val = _ifelse(_exists(e1_sym), e1_sym, _f32(0));
    auto e3_val = _ifelse(_exists(e3_sym), e3_sym, _f32(0));
    auto e4_val = _ifelse(_exists(e4_sym), e4_sym, _f32(0));

    auto res = (t_sym / _f32(8)) * (e0_val + (_f32(2) * e1_val) - (_f32(2) * e3_val) - e4_val);
    auto res_sym = _sym("res", res);

    return _op(
        _iter(0, p),
        Params{in, beat},
        SymTable{
            {e0_sym, e0},
            {e1_sym, e1},
            {e3_sym, e3},
            {e4_sym, e4},
            {t_sym, t},
            {res_sym, res}
        },
        _true(),
        res_sym);
}

Op _MovingSqAvg(_sym in, int64_t p, int64_t window)
{
    auto out = _out(types::STRUCT<float, float>());

    auto head = in[_pt(0)];
    auto head_sym = _sym("head", head);
    auto tail = in[_pt(-window)];
    auto tail_sym = _sym("tail", tail);
    auto o = out[_pt(-p)];
    auto o_sym = _sym("o", o);
    auto state = _ifelse(_exists(o_sym), o_sym, _new(vector<Expr>{_f32(0), _f32(0)}));
    auto state_sym = _sym("state", state);

    auto sum = state_sym << 0;
    auto count = state_sym << 1;

    auto new_sum = sum
        + _ifelse(_exists(head_sym), head_sym * head_sym, _f32(0))
        - _ifelse(_exists(tail_sym), tail_sym * tail_sym, _f32(0));
    auto new_count = count
        + _ifelse(_exists(head_sym), _f32(1), _f32(0))
        - _ifelse(_exists(tail_sym), _f32(1), _f32(0));

    auto res = _new(vector<Expr>{new_sum, new_count});
    auto res_sym = _sym("res", res);

    auto ma_op = _op(
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

    return ma_op;
}

Op _PanTom(_sym in, int64_t p, int64_t window, int64_t scale)
{
    auto low_state = _sym("low_state", tilt::Type(types::FLOAT32, _iter(0, -1)));
    auto high_state = _sym("high_state", tilt::Type(types::FLOAT32, _iter(0, -1)));
    auto ma_state = _sym("ma_state", tilt::Type(types::STRUCT<float, float>(), _iter(0, -1)));

    auto win = in[_win(-p * scale - window, 0)];
    auto win_sym = _sym("win", win);

    auto lp = _LowPass(win_sym, p);
    auto lp_sym = _sym("lp", lp);
    auto hp = _HighPass(lp_sym, p);
    auto hp_sym = _sym("hp", hp);
    auto derv = _Derive(hp_sym, p);
    auto derv_sym = _sym("derv", derv);
    auto ma = _MovingSqAvg(derv_sym, p, window);
    auto ma_sym = _sym("ma", ma);
    auto val = _f32(0);
    auto val_sym = _sym("val", val);
    auto sel = _Select(ma_sym, val_sym, [](_sym e, _sym val) {
        auto sum = e << 0;
        auto count = e << 1;
        return sum / count;
    });
    auto sel_sym = _sym("sel", sel);

    return _op(
        _iter(0, p * scale),
        Params{in, low_state, high_state, ma_state},
        SymTable{
            {win_sym, win},
            {lp_sym, lp},
            {hp_sym, hp},
            {derv_sym, derv},
            {ma_sym, ma},
            {val_sym, val},
            {sel_sym, sel}
        },
        _true(),
        sel_sym,
        Aux{
            {lp_sym, low_state},
            {hp_sym, high_state},
            {ma_sym, ma_state}
        });
}

struct AvgState {
    float sum;
    float count;
};

class PeakBench : public Benchmark {
public:
    PeakBench(int64_t period, int64_t window, int64_t scale, int64_t size) :
        period(period), window(window), scale(scale), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _PanTom(in_sym, period, window, scale);
    }

    void init() final
    {
        in_reg = create_reg<float>(size);
        low_state_reg = create_reg<float>(scale);
        high_state_reg = create_reg<float>(scale);
        ma_state_reg = create_reg<AvgState>(scale);
        out_reg = create_reg<float>(size);

        SynthData<float> dataset(period, size);
        dataset.fill(&in_reg);
    }

    void execute(intptr_t addr) final
    {
        auto query = (region_t* (*)(ts_t, ts_t, region_t*, region_t*, region_t*, region_t*, region_t*)) addr;
        query(0, period * size, &out_reg, &in_reg, &low_state_reg, &high_state_reg, &ma_state_reg);
    }

    void release() final
    {
        for (int i = 0; i < size; i++) {
            cout << "(" << in_reg.tl[i].t << "," << in_reg.tl[i].t + in_reg.tl[i].d << ") "
                << reinterpret_cast<float*>(in_reg.data)[i] << " -> "
                << "(" << out_reg.tl[i].t << "," << out_reg.tl[i].t + out_reg.tl[i].d << ")"
                << reinterpret_cast<float*>(out_reg.data)[i] << endl;
        }
        release_reg(&in_reg);
        release_reg(&low_state_reg);
        release_reg(&high_state_reg);
        release_reg(&ma_state_reg);
        release_reg(&out_reg);
    }

    dur_t period;
    int64_t window;
    int64_t scale;
    int64_t size;
    region_t in_reg;
    region_t low_state_reg;
    region_t high_state_reg;
    region_t ma_state_reg;
    region_t out_reg;
};

#endif  // TILT_BENCH_INCLUDE_TILT_PEAK_H_
