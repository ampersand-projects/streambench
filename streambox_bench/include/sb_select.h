#ifndef STREAMBOX_BENCH_INCLUDE_SELECT_BENCHMARK_H_
#define STREAMBOX_BENCH_INCLUDE_SELECT_BENCHMARK_H_

#include <chrono>

#include <core/Pipeline.h>
#include <core/EvaluationBundleContext.h>
#include "Sink/Sink.h"

#include "streambench/Source/BoundedInMem.h"
#include "streambench/Mapper/ProjectMapper.h"

#include <sb_bench.h>

class SelectBench : public Benchmark {
private:
    int64_t dur;
    function<temporal_event(temporal_event)> projector;
public:
    int64_t run_benchmark() override {
        BoundedInMem<temporal_event, BundleT> bound(
            "Bounded-inmem", dur,
            config.records_total,
            config.records_per_interval
        );
        ProjectMapper<temporal_event, temporal_event, BundleT> mapper("[project-mapper]", projector);
        RecordBundleSink<temporal_event> sink("sink");

        Pipeline* p = Pipeline::create(NULL);
        source_transform(bound);
        connect_transform(bound, mapper);
        connect_transform(mapper, sink);
        
        EvaluationBundleContext eval(1, config.cores);

        auto start_time = chrono::high_resolution_clock::now();
        eval.runSimple(p);
        auto end_time = chrono::high_resolution_clock::now();
        
        int64_t time = chrono::duration_cast<chrono::microseconds>(end_time - start_time).count();
        return time;
    }

    SelectBench(bench_pipeline_config config, int64_t dur, function<temporal_event(temporal_event)> projector) :
        Benchmark(config),
        dur(dur),
        projector(projector)
    {}
};


#endif // STREAMBOX_BENCH_INCLUDE_SELECT_BENCHMARK_H_