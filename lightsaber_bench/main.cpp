#include <ls_select.h>
#include <ls_where.h>
#include <ls_aggregate.h>
#include <ls_alterdur.h>
#include <ls_yahoo.h>

int main(int argc, const char **argv) {
    std::string testcase = (argc > 1) ? argv[1] : "select";
    int64_t size = (argc > 2) ? atoi(argv[2]) : 50000000;
    int64_t runs = (argc > 3) ? atoi(argv[3]) : 100;
    int64_t period = 1;
    SystemConf::getInstance().BATCH_SIZE = 200000; // This means the input_size (size * sizeof(InputSchema)) must be multiple of 200,000
    SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = size * sizeof(Benchmark::InputSchema);

    std::unique_ptr<Benchmark> benchmarkQuery {};
    if (testcase == "select") {
        benchmarkQuery = std::make_unique<SelectBench>(60);
    } else if (testcase == "where") {
        benchmarkQuery = std::make_unique<WhereBench>(60);
    } else if (testcase == "aggregate") {
        benchmarkQuery = std::make_unique<AggregateBench>(1000);
    } else if (testcase == "alterdur") {
        benchmarkQuery = std::make_unique<AlterDurBench>(1, 60);
    } else if (testcase == "yahoo") {
        SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = size * sizeof(YahooBench::YahooSchema);
        benchmarkQuery = std::make_unique<YahooBench>(1000);
    } else {
        throw std::runtime_error("Invalid testcase");
    }

    benchmarkQuery->PopulateBufferWithData(size, period);
    for (size_t i = 0; i < runs; i++) {
        benchmarkQuery->runBenchmark();
    }

    return 0;
}