#ifndef STREAMBOX_BENCH_INCLUDE_JOIN_BENCHMARK_H_
#define STREAMBOX_BENCH_INCLUDE_JOIN_BENCHMARK_H_

#include <chrono>

#include <core/Pipeline.h>
#include <core/EvaluationBundleContext.h>
#include "Sink/Sink.h"
#include "Join/Join.h"

#include "streambench/Source/BoundedInMem.h"
#include "streambench/Mapper/TemporalKVMapper.h"

#include <sb_bench.h>

class JoinBench : public Benchmark {
private:
    int64_t dur;
public:
    int64_t run_benchmark() override {
        BoundedInMem<temporal_event, BundleT> bound(
            "Bounded-inmem", dur,
            config.records_total,
            config.records_per_interval
        );
        TemporalKVMapper<temporal_event, pair<long, long>, BundleT>
            mapper0("mapper0");
        TemporalKVMapper<temporal_event, pair<long, long>, BundleT>
            mapper1("mapper1");
        Join<pair<long, long>, BundleT, BundleT> join("[join]", seconds(1));
        RecordBundleSink<pair<long, vector<long>>> sink1("sink1");
        RecordBundleSink<pair<long, vector<long>>> sink2("sink2");

        mapper0.set_side_info(SIDE_INFO_L); //left side
        mapper1.set_side_info(SIDE_INFO_R); //right side
        join.set_side_info(SIDE_INFO_J);
        sink1.set_side_info(SIDE_INFO_JD);
        sink2.set_side_info(SIDE_INFO_JDD);

        Pipeline* p = Pipeline::create(NULL);
        source_transform_1to2(bound);
        connect_transform_1to2(bound, mapper0, mapper1);
        connect_transform_2to1(mapper0, mapper1, join);
	    connect_transform(join, sink1);
        connect_transform(sink1, sink2);

        EvaluationBundleContext eval(1, config.cores);

        auto start_time = chrono::high_resolution_clock::now();
        eval.runSimple(p);
        auto end_time = chrono::high_resolution_clock::now();
        
        int64_t time = chrono::duration_cast<chrono::microseconds>(end_time - start_time).count();
        return time;
    }

    JoinBench(bench_pipeline_config config, int64_t dur) :
        Benchmark(config),
        dur(dur)
    {}
};


#endif // STREAMBOX_BENCH_INCLUDE_JOIN_BENCHMARK_H_