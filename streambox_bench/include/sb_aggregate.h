#ifndef STREAMBOX_BENCH_INCLUDE_AGGREGATE_BENCHMARK_H_
#define STREAMBOX_BENCH_INCLUDE_AGGREGATE_BENCHMARK_H_

#include <chrono>

#include <core/Pipeline.h>
#include <core/EvaluationBundleContext.h>
#include "Sink/Sink.h"
#include "WinSum/WinSum_addlong.h"

#include "streambench/Source/BoundedInMem.h"
#include "streambench/Mapper/TemporalWinMapper.h"

#include <sb_bench.h>

class AggregateBench : public Benchmark {
private:
    int64_t dur;
public:
    int64_t run_benchmark() override {
        BoundedInMem<temporal_event, BundleT> bound(
            "Bounded-inmem", dur,
            config.records_total,
            config.records_per_interval
        );
        TemporalWinMapper<temporal_event, long, BundleT> mapper("winmapper", seconds(1), ptime(boost::gregorian::date(2020, Jan, 1)));
        WinSum_addlong<long, long> agg ("agg", 1);
        RecordBundleSink<long> sink("sink");

        Pipeline* p = Pipeline::create(NULL);
        source_transform(bound);
        connect_transform(bound, mapper);
        connect_transform(mapper, agg);
        connect_transform(agg, sink);

        EvaluationBundleContext eval(1, config.cores);

        auto start_time = chrono::high_resolution_clock::now();
        eval.runSimple(p);
        auto end_time = chrono::high_resolution_clock::now();
        
        int64_t time = chrono::duration_cast<chrono::microseconds>(end_time - start_time).count();
        return time;
    }

    AggregateBench(bench_pipeline_config config, int64_t dur) :
        Benchmark(config),
        dur(dur)
    {}
};


#endif // STREAMBOX_BENCH_INCLUDE_AGGREGATE_BENCHMARK_H_