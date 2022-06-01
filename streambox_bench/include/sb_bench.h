#ifndef STREAMBOX_BENCH_INCLUDE_BENCHMARK_H_
#define STREAMBOX_BENCH_INCLUDE_BENCHMARK_H_

#include <test/test-common.h>

template<class T>
using BundleT = RecordBundle<T>;

struct bench_pipeline_config {
    unsigned long records_total;
	unsigned long records_per_interval;
	unsigned long cores;
};

class Benchmark {

protected:
    bench_pipeline_config config;

public:
    Benchmark(bench_pipeline_config config) :
        config(config)
    {}
    virtual int64_t run_benchmark() = 0;
};

#endif // STREAMBOX_BENCH_INCLUDE_BENCHMARK_H_