#include <iostream>
#include <memory>
#include <cstdlib>
#include <chrono>

#include "tilt/codegen/printer.h"
#include "tilt/codegen/loopgen.h"
#include "tilt/codegen/llvmgen.h"
#include "tilt/codegen/vinstr.h"
#include "tilt/engine/engine.h"

using namespace tilt;
using namespace tilt::tilder;
using namespace std;
using namespace std::chrono;

Op _Select(_sym in, function<Expr(Expr)> sel_expr)
{
    auto e = in[_pt(0)];
    auto e_sym = _sym("e", e);
    auto sel = sel_expr(e_sym);
    auto sel_sym = _sym("sel", sel);
    auto sel_op = _op(
        _iter(0, 1),
        Params{ in },
        SymTable{ {e_sym, e}, {sel_sym, sel} },
        _exists(e_sym),
        sel_sym);
    return sel_op;
}

int main(int argc, char** argv)
{
    int dlen = (argc > 1) ? atoi(argv[1]) : 20;
    int len = (argc > 2) ? atoi(argv[2]) : 10;
    uint32_t dur = 5;

    // input stream
    auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));

    auto query_op = _Select(in_sym, [](Expr expr) { return _add(expr, _f32(10)); });
    auto query_op_sym = _sym("query", query_op);
    cout << endl << "TiLT IR:" << endl;
    cout << IRPrinter::Build(query_op) << endl;

    auto loop = LoopGen::Build(query_op_sym, query_op.get());
    cout << endl << "Loop IR:" << endl;
    cout << IRPrinter::Build(loop);

    auto jit = ExecEngine::Get();
    auto& llctx = jit->GetCtx();
    auto llmod = LLVMGen::Build(loop, llctx);
    cout << endl << "LLVM IR:" << endl;
    cout << IRPrinter::Build(llmod.get()) << endl;

    jit->AddModule(move(llmod));

    auto loop_addr = (region_t* (*)(ts_t, ts_t, region_t*, region_t*)) jit->Lookup(loop->get_name());

    auto size = get_buf_size(dlen);
    auto in_tl = new ival_t[size];
    auto in_data = new float[size];
    auto out_tl = new ival_t[size];
    auto out_data = new float[size];

    region_t in_reg;
    init_region(&in_reg, 0, size, in_tl, reinterpret_cast<char*>(in_data));
    for (int i = 0; i < size; i++) {
        auto t = dur*i;
        in_reg.et = t;
        in_reg.ei++;
        in_tl[in_reg.ei] = {t, dur};
        in_data[in_reg.ei] = i%1000 + 1;
        out_tl[i] = {0, 0};
        out_data[i] = 0;
    }

    region_t out_reg;
    init_region(&out_reg, 0, size, out_tl, reinterpret_cast<char*>(out_data));

    cout << "Query execution: " << endl;
    auto start_time = high_resolution_clock::now();
    auto* res_reg = loop_addr(0, dur*dlen, &out_reg, &in_reg);
    auto end_time = high_resolution_clock::now();

    int out_count = dlen;
    if (argc == 1) {
        for (int i = 0; i < size; i++) {
            cout << "(" << in_tl[i].t << "," << in_tl[i].t + in_tl[i].d << ") " << in_data[i] << " -> "
                << "(" << out_tl[i].t << "," << out_tl[i].t + out_tl[i].d << ")" << out_data[i] << endl;
        }
    }

    auto time = duration_cast<microseconds>(end_time - start_time).count();
    cout << "Data size: " << out_count << " Time: " << time << endl;

    return 0;
}
