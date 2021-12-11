#include <ls_select.h>
#include <ls_where.h>
#include <ls_aggregate.h>
#include <ls_alterdur.h>

int main(int argc, const char **argv) {
    std::string testcase = (argc > 1) ? argv[1] : "select";
    int64_t size = (argc > 2) ? atoi(argv[2]) : 10000000;
    int64_t runs = (argc > 3) ? atoi(argv[3]) : 1;
    int64_t period = 1;
    SystemConf::getInstance().BATCH_SIZE = 100000; // This means the input_size (size * sizeof(InputSchema)) must be multiple of 100,000
    SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = size * sizeof(Benchmark::InputSchema);

    double time = 0;
    std::unique_ptr<Benchmark> benchmarkQuery {};
    if (testcase == "select") {
        benchmarkQuery = std::make_unique<SelectBench>(60);
    } else if (testcase == "where") {
        benchmarkQuery = std::make_unique<WhereBench>(60);
    } else if (testcase == "aggregate") {
        benchmarkQuery = std::make_unique<AggregateBench>(1000);
    } else if (testcase == "alterdur") {
        benchmarkQuery = std::make_unique<AlterDurBench>(1, 60);
    } else {
        throw std::runtime_error("Invalid testcase");
    }

    for (int64_t i = 0; i < runs; i++) {
        time += benchmarkQuery->runBenchmark(size, period);
    }

    std::cout << "Testcase: " << testcase <<", Size: " << size * runs
        << ", Time: " << std::setprecision(3) << time / 1000000 << std::endl;

    return 0;
}