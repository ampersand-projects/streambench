#include <string.h>
#include <iostream>
#include <thread>
#include <iomanip>

#include "sb_select.h"
#include "sb_where.h"
#include "sb_aggregate.h"

int main(int argc, char *argv[])
{
	string testcase = (argc > 1) ? argv[1] : "select";
    long unsigned int records_total = (argc > 2) ? atoi(argv[2]) : 10000000;
	long unsigned int records_per_interval = (argc > 3) ? atoi(argv[3]) : 1000000;

	bench_pipeline_config config = {
		.records_total = records_total,
		.records_per_interval = records_per_interval,
		.cores = thread::hardware_concurrency() - 1
	};

	print_config();

	double time = 0;
    if (testcase == "select") {
		auto projector = [](temporal_event e) {
            temporal_event e1 {e.dur, e.payload + 1};
            return e1;
        };
        SelectBench benchmark(config, 1, projector);
		time = benchmark.run_benchmark();
    } else if (testcase == "where") {
        auto filter = [](temporal_event e) {
            return e.payload > 0;
        };
        WhereBench benchmark(config, 1, filter);
		time = benchmark.run_benchmark();
    } else if (testcase == "alterdur") {
        auto projector = [](temporal_event e) {
            temporal_event e1 {e.dur + 1, e.payload};
            return e1;
        };
        SelectBench benchmark(config, 1, projector);
		time = benchmark.run_benchmark();
    } else if (testcase == "aggregate") {
        AggregateBench benchmark(config, 1);
		time = benchmark.run_benchmark();
    } else {
        throw runtime_error("Invalid testcase");
    }

    cout << fixed;
    cout << "Testcase: " << testcase <<", Size: " << records_total
    	<< ", Time: " << setprecision(3) << time / 1000000 << endl;

    return 0;
}