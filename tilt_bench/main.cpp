#include <iostream>

#include "tilt_norm.h"
#include "tilt_ma.h"
#include "tilt_rsi.h"
#include "tilt_qty.h"

using namespace std;

Op TestSel(_sym in)
{
    auto cur = in[_pt(0)];
    auto cur_sym = _sym("cur", cur);
    auto prev = in[_pt(-1)];
    auto prev_sym = _sym("prev", prev);

    auto res = _new(vector<Expr>{prev_sym, cur_sym});
    auto res_sym = _sym("res", res);

    return _op(
        _iter(0, 1),
        Params{in},
        SymTable{
            {cur_sym, cur},
            {prev_sym,  prev},
            {res_sym, res}
        },
        _true(),
        res_sym);
}

Op _TestOp(_sym in, int64_t window)
{
    auto state = _sym("state", tilt::Type(types::FLOAT32, _iter(0, -1)));

    auto win = in[_win(-window, 0)];
    auto win_sym = _sym("win", win);
    auto val = _f32(0);
    auto val_sym = _sym("val", val);

    auto sel = _Select(win_sym, val_sym, [](_sym e, _sym v) { return e + _f32(10); });
    auto sel_sym = _sym("sel", sel);

    auto test_sel = TestSel(sel_sym);
    auto test_sel_sym = _sym("test", test_sel);

    return _op(
        _iter(0, window),
        Params{in, state},
        SymTable{
            {win_sym, win},
            {val_sym, val},
            {sel_sym, sel},
            {test_sel_sym, test_sel}
        },
        _true(),
        test_sel_sym,
        Aux{
            {sel_sym, state}
        });
}

struct Data {
    float a;
    float b;

    string str() const
    {
        return "{" + to_string(a) + ", " + to_string(b) + "}";
    }
};

class TestBench : public Benchmark {
private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _TestOp(in_sym, 5);
    }

    void init() final
    {
        auto size = 100;
        auto period = 1;

        in_reg = create_reg<float>(size);
        state_reg = create_reg<float>(6);
        out_reg = create_reg<Data>(size);

        SynthData<float> dataset(period, size);
        dataset.fill(&in_reg);
    }

    void execute(intptr_t addr) final
    {
        auto size = 100;
        auto period = 1;

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
                << reinterpret_cast<Data*>(out_reg.data)[i].str() << endl;
        }
        release_reg(&in_reg);
        release_reg(&out_reg);
    }

    region_t in_reg;
    region_t state_reg;
    region_t out_reg;
};


int main(int argc, char** argv)
{
    LargeQtyBench bench(1, 5, 10, 100);
    auto time = bench.run();
    cout << "Time: " << time << endl;

    return 0;
}
