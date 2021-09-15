#ifndef TILT_BENCH_INCLUDE_TILT_BENCH_H_
#define TILT_BENCH_INCLUDE_TILT_BENCH_H_

#include <memory>
#include <cstdlib>
#include <chrono>

#include "tilt/codegen/loopgen.h"
#include "tilt/codegen/llvmgen.h"
#include "tilt/codegen/vinstr.h"
#include "tilt/engine/engine.h"

using namespace std;
using namespace std::chrono;
using namespace tilt;
using namespace tilt::tilder;

template<typename T>
class Dataset {
public:
    virtual void fill(region_t*) = 0;
};

template<typename T>
class SynthData : public Dataset<T> {
public:
    SynthData(dur_t period, int64_t len) : period(period), len(len) {}

    void fill(region_t* reg) final
    {
        auto data = reinterpret_cast<T*>(reg->data);
        for (int i = 0; i < len; i++) {
            auto t = period * i;
            reg->et = t;
            reg->ei++;
            reg->tl[reg->ei] = {t, period};
            data[reg->ei] = (T) (i % 1000 + 1);
        }
    }

private:
    dur_t period;
    int64_t len;
};

class Benchmark {
public:
    virtual void init() = 0;
    virtual void release() = 0;

    int64_t run()
    {
        auto query_op = query();
        auto query_op_sym = _sym("query", query_op);

        auto loop = LoopGen::Build(query_op_sym, query_op.get());

        auto jit = ExecEngine::Get();
        auto& llctx = jit->GetCtx();
        auto llmod = LLVMGen::Build(loop, llctx);
        jit->AddModule(move(llmod));

        init();

        auto start_time = high_resolution_clock::now();
        execute(jit->Lookup(loop->get_name()));
        auto end_time = high_resolution_clock::now();

        release();

        return duration_cast<microseconds>(end_time - start_time).count();
    }

    template<typename T>
    static region_t create_reg(int64_t size)
    {
        region_t reg;
        auto buf_size = get_buf_size(size);
        auto tl = new ival_t[buf_size];
        auto data = new T[buf_size];
        init_region(&reg, 0, buf_size, tl, reinterpret_cast<char*>(data));
        return reg;
    }

    static void release_reg(region_t* reg)
    {
        delete [] reg->tl;
        delete [] reg->data;
    }

protected:
    virtual Op query() = 0;
    virtual void execute(intptr_t) = 0;
};

#endif  // TILT_BENCH_INCLUDE_TILT_BENCH_H_
