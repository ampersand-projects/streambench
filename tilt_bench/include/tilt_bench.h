#ifndef TILT_BENCH_INCLUDE_TILT_BENCH_H_
#define TILT_BENCH_INCLUDE_TILT_BENCH_H_

#include <memory>
#include <cstdlib>
#include <chrono>

#include "tilt/codegen/loopgen.h"
#include "tilt/codegen/llvmgen.h"
#include "tilt/codegen/vinstr.h"
#include "tilt/engine/engine.h"

#include "tilt_norm.h"
#include "tilt_ma.h"

using namespace std;
using namespace std::chrono;

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

class NormBench : public Benchmark {
public:
    NormBench(int64_t window, dur_t period, int64_t size) :
        window(window), period(period), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _Norm(in_sym, window);
    }

    void init() final
    {
        in_reg = create_reg<float>(size);
        out_reg = create_reg<float>(size);

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

    int64_t window;
    dur_t period;
    int64_t size;
    region_t in_reg;
    region_t out_reg;
};

struct MOCAData {
    float a;
    float b;
    float c;
    float d;
};

class MOCABench : public Benchmark {
public:
    MOCABench(int64_t period, int64_t w_short, int64_t w_long, int64_t scale, int64_t size) :
        period(period), w_short(w_short), w_long(w_long), scale(scale), size(size)
    {}

private:
    Op query() final
    {
        auto in_sym = _sym("in", tilt::Type(types::FLOAT32, _iter(0, -1)));
        return _MACrossOver(in_sym, period, w_short, w_long, scale);
    }

    void init() final
    {
        in_reg = create_reg<float>(size);
        state_reg = create_reg<MOCAData>(size);
        out_reg = create_reg<bool>(size);

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
        for (int i = 0; i < size; i++) {
            cout << "(" << in_reg.tl[i].t << "," << in_reg.tl[i].t + in_reg.tl[i].d << ") "
                << reinterpret_cast<float*>(in_reg.data)[i] << " -> "
                << "(" << out_reg.tl[i].t << "," << out_reg.tl[i].t + out_reg.tl[i].d << ")"
                << reinterpret_cast<bool*>(out_reg.data)[i] << endl;
        }

        release_reg(&in_reg);
        release_reg(&state_reg);
        release_reg(&out_reg);
    }

    dur_t period;
    int64_t w_short;
    int64_t w_long;
    int64_t scale;
    int64_t size;
    region_t in_reg;
    region_t state_reg;
    region_t out_reg;
};

#endif  // TILT_BENCH_INCLUDE_TILT_BENCH_H_
