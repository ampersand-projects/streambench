#include <iostream>
#include <iomanip>

#include "tilt_select.h"
#include "tilt_where.h"
#include "tilt_aggregate.h"
#include "tilt_norm.h"
#include "tilt_ma.h"
#include "tilt_rsi.h"
#include "tilt_qty.h"
#include "tilt_impute.h"
#include "tilt_peak.h"
#include "tilt_resample.h"

using namespace std;

int main(int argc, char** argv)
{
    string testcase = (argc > 1) ? argv[1] : "select";
    int64_t size = (argc > 2) ? atoi(argv[2]) : 100000000;

    double time = 0;

    if (testcase == "select") {
        SelectBench bench(1, size);
        time = bench.run();
    } else if (testcase == "where") {
        WhereBench bench(1, size);
        time = bench.run();
    } else if (testcase == "aggregate") {
        AggregateBench bench(1, size, 100);
        time = bench.run();
    } else if (testcase == "normalize") {
        NormBench bench(1, 10000, size);
        time = bench.run();
    } else if (testcase == "fillmean") {
        ImputeBench bench(1, 10000, size);
        time = bench.run();
    } else if (testcase == "resample") {
        ResampleBench bench(4, 5, 1000, size);
        time = bench.run();
    } else if (testcase == "algotrading") {
        MOCABench bench(1, 20, 50, 100, size);
        time = bench.run();
    } else if (testcase == "rsi") {
        RSIBench bench(1, 14, 100, size);
        time = bench.run();
    } else if (testcase == "largeqty") {
        LargeQtyBench bench(1, 10, 100, size);
        time = bench.run();
    } else if (testcase == "pantom") {
        PeakBench bench(1, 30, 100, size);
        time = bench.run();
    } else {
        throw runtime_error("Invalid testcase");
    }

    cout << "Testcase: " << testcase <<", Size: " << size
        << ", Time: " << setprecision(3) << time / 1000000 << endl;

    return 0;
}
