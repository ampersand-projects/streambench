#ifndef TILT_BENCH_INCLUDE_TILT_BASE_H_
#define TILT_BENCH_INCLUDE_TILT_BASE_H_

#include "tilt/builder/tilder.h"

using namespace tilt;
using namespace tilt::tilder;

Op _Select(_sym in, _sym val, function<Expr(_sym, _sym)> selector)
{
    auto e = in[_pt(0)];
    auto e_sym = _sym("e", e);
    auto res = selector(e_sym, val);
    auto res_sym = _sym("res", res);
    auto sel_op = _op(
        _iter(0, 1),
        Params{in, val},
        SymTable{{e_sym, e}, {res_sym, res}},
        _exists(e_sym),
        res_sym);
    return sel_op;
}

Expr _Count(_sym win)
{
    auto acc = [](Expr s, Expr st, Expr et, Expr d) { return _add(s, _f32(1)); };
    return _red(win, _f32(0), acc);
}

Expr _Sum(_sym win)
{
    auto acc = [](Expr s, Expr st, Expr et, Expr d) { return _add(s, d); };
    return _red(win, _f32(0), acc);
}

Op _WindowAvg(_sym in, int64_t w)
{
    auto window = in[_win(-w, 0)];
    auto window_sym = _sym("win", window);
    auto count = _Count(window_sym);
    auto count_sym = _sym("count", count);
    auto sum = _Sum(window_sym);
    auto sum_sym = _sym("sum", sum);
    auto avg = sum_sym / count_sym;
    auto avg_sym = _sym("avg", avg);
    auto wc_op = _op(
        _iter(0, w),
        Params{ in },
        SymTable{ {window_sym, window}, {count_sym, count}, {sum_sym, sum}, {avg_sym, avg} },
        _true(),
        avg_sym);
    return wc_op;
}

Expr _Average(_sym win, function<Expr(Expr)> selector)
{
    auto acc = [selector](Expr s, Expr st, Expr et, Expr d) {
        auto sum = _get(s, 0);
        auto count = _get(s, 1);
        return _new(vector<Expr>{_add(sum, selector(d)), _add(count, _f32(1))});
    };
    return _red(win, _new(vector<Expr>{_f32(0), _f32(0)}), acc);
}

Op _Join(_sym left, _sym right)
{
    auto e_left = left[_pt(0)];
    auto e_left_sym = _sym("left", e_left);
    auto e_right = right[_pt(0)];
    auto e_right_sym = _sym("right", e_right);
    auto norm = e_left_sym - e_right_sym;
    auto norm_sym = _sym("norm", norm);
    auto left_exist = _exists(e_left_sym);
    auto right_exist = _exists(e_right_sym);
    auto join_cond = left_exist && right_exist;
    auto join_op = _op(
        _iter(0, 1),
        Params{ left, right },
        SymTable{
            {e_left_sym, e_left},
            {e_right_sym, e_right},
            {norm_sym, norm},
        },
        join_cond,
        norm_sym);
    return join_op;
}

#endif  // TILT_BENCH_INCLUDE_TILT_BASE_H_
