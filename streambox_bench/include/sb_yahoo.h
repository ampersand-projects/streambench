#ifndef STREAMBOX_BENCH_INCLUDE_YAHOO_BENCHMARK_H_
#define STREAMBOX_BENCH_INCLUDE_YAHOO_BENCHMARK_H_

#include <chrono>

#include <core/Pipeline.h>
#include <core/EvaluationBundleContext.h>
#include "Sink/Sink.h"

#include "streambench/Source/BoundedInMem.h"
#include "streambench/Mapper/ProjectMapper.h"
#include "streambench/Mapper/FilterMapper.h"
#include "streambench/Mapper/TemporalWinMapper.h"
#include "streambench/WinSum/WinCount.h"

#include <sb_bench.h>

class YahooBench : public Benchmark {
private:
    int64_t dur;
public:
    int64_t run_benchmark() override {
        BoundedInMem<yahoo_event, BundleT> bound(
            "Bounded-inmem", dur,
            config.records_total,
            config.records_per_interval
        );
        auto filter = [](yahoo_event e) {
            return e.event_type == 1l;
        };
        FilterMapper<yahoo_event, BundleT> filter_mapper("[filter-mapper]", filter);
        auto projector = [](yahoo_event e) {
            yahoo_event_projected projected { e.campaign_id, e.event_type };
            return projected;
        };
        ProjectMapper<yahoo_event, yahoo_event_projected, BundleT>
            project_mapper("[project-mapper]", projector);
        TemporalWinMapper<yahoo_event_projected, yahoo_event_projected, BundleT>
            win_mapper("[win-mapper]", seconds(1));
        WinCount<yahoo_event_projected, uint64_t> agg("agg", 1);
        RecordBundleSink<uint64_t> sink("sink");

        Pipeline* p = Pipeline::create(NULL);
        source_transform(bound);
        connect_transform(bound, filter_mapper);
        connect_transform(filter_mapper, project_mapper);
        connect_transform(project_mapper, win_mapper);
        connect_transform(win_mapper, agg);
        connect_transform(agg, sink);
        
        EvaluationBundleContext eval(1, config.cores);

        auto start_time = chrono::high_resolution_clock::now();
        eval.runSimple(p);
        auto end_time = chrono::high_resolution_clock::now();
        
        int64_t time = chrono::duration_cast<chrono::microseconds>(end_time - start_time).count();
        return time;
    }

    YahooBench(bench_pipeline_config config, int64_t dur) :
        Benchmark(config),
        dur(dur)
    {}
};


#endif // STREAMBOX_BENCH_INCLUDE_YAHOO_BENCHMARK_H_