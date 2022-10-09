#include <ls_select.h>
#include <ls_where.h>
#include <ls_aggregate.h>
#include <ls_alterdur.h>
#include <ls_yahoo.h>

int main(int argc, const char **argv) {
    std::string testcase = (argc > 1) ? argv[1] : "select";
    int64_t size = (argc > 2) ? atoi(argv[2]) : 1000000; // In # of events
    int64_t runs = (argc > 3) ? atoi(argv[3]) : 10000;
    int64_t cores = (argc > 4) ? atoi(argv[4]) : 1;
    int64_t period = 1;
    SystemConf::getInstance().BATCH_SIZE = 131072; // In Bytes
    SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = 8 * size * sizeof(Benchmark::InputSchema); // In Bytes, the coefficient is worth tuning
    SystemConf::getInstance().WORKER_THREADS = cores;

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
        SystemConf::getInstance().BATCH_SIZE = 1048576;
        SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = 8 * size * sizeof(YahooBench::YahooSchema);
        benchmarkQuery = std::make_unique<YahooBench>(1000);
    } else {
        throw std::runtime_error("Invalid testcase");
    }

    double time = benchmarkQuery->runBenchmark(size, period, runs);

    std::cout << "Testcase: " << testcase << ", Size: " << size * runs
        << ", Time: " << std::setprecision(3) << time / 1000000 << std::endl;

    return 0;
}