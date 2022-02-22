#include <iostream>
#include <iomanip>
#include <sys/resource.h>

#include <data_loader.h>
#include <taxi_data_parser.h>

#include "tilt_select.h"
#include "tilt_where.h"
#include "tilt_aggregate.h"
#include "tilt_alterdur.h"
#include "tilt_innerjoin.h"
#include "tilt_outerjoin.h"
#include "tilt_norm.h"
#include "tilt_ma.h"
#include "tilt_rsi.h"
#include "tilt_qty.h"
#include "tilt_impute.h"
#include "tilt_peak.h"
#include "tilt_resample.h"
#include "tilt_kurt.h"
#include "tilt_eg.h"

using namespace std;

int main(int argc, char** argv)
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    const rlim_t kStackSize = 2 * 1024 * 1024 * 1024;   // min stack size = 2 GB
    struct rlimit rl;
    int result;

    result = getrlimit(RLIMIT_STACK, &rl);
    if (result == 0) {
        if (rl.rlim_cur < kStackSize) {
            rl.rlim_cur = kStackSize;
            result = setrlimit(RLIMIT_STACK, &rl);
            if (result != 0) {
                cerr << "setrlimit returned result = " << result << endl;
            }
        }
    }

    data_loader<stream::taxi_fare> loader;
    while (true) {
        stream::taxi_fare fare;
        loader.load_data(fare);
        cout << fare << endl;
    }

    string testcase = (argc > 1) ? argv[1] : "select";
    int64_t size = (argc > 2) ? atoi(argv[2]) : 100000000;
    int64_t period = 1;

    double time = 0;

    if (testcase == "select") {
        SelectBench bench(period, size);
        time = bench.run();
    } else if (testcase == "where") {
        WhereBench bench(period, size);
        time = bench.run();
    } else if (testcase == "aggregate") {
        AggregateBench bench(period, size, 1000 * period);
        time = bench.run();
    } else if (testcase == "alterdur") {
        AlterDurBench bench(3, 2, size);
        time = bench.run();
    } else if (testcase == "innerjoin") {
        InnerJoinBench bench(period, period, size);
        time = bench.run();
    } else if (testcase == "outerjoin") {
        OuterJoinBench bench(period, period, size);
        time = bench.run();
    } else if (testcase == "normalize") {
        NormBench bench(period, 10000, size);
        time = bench.run();
    } else if (testcase == "fillmean") {
        ImputeBench bench(period, 10000, size);
        time = bench.run();
    } else if (testcase == "resample") {
        ResampleBench bench(4, 5, 1000, size);
        time = bench.run();
    } else if (testcase == "algotrading") {
        MOCABench bench(period, 20, 50, 100, size);
        time = bench.run();
    } else if (testcase == "rsi") {
        RSIBench bench(period, 14, 100, size);
        time = bench.run();
    } else if (testcase == "largeqty") {
        LargeQtyBench bench(period, 10, 100, size);
        time = bench.run();
    } else if (testcase == "pantom") {
        PeakBench bench(period, 30, 100, size);
        time = bench.run();
    } else if (testcase == "kurtosis") {
        KurtBench bench(period, 100, size);
        time = bench.run();
    } else if (testcase == "eg1") {
        Eg1Bench bench(period, size, 10, 20, size);
        time = bench.run();
    } else if (testcase == "eg2") {
        Eg2Bench bench(period, size, 10, 20, size);
        time = bench.run();
    } else if (testcase == "eg3") {
        Eg3Bench bench(period, size, 10, 20, size);
        time = bench.run();
    } else if (testcase == "eg4") {
        Eg4Bench bench(period, size, 10, 20, size);
        time = bench.run();
    } else if (testcase == "eg5") {
        Eg5Bench bench(period, size, 10, 20, size);
        time = bench.run();
    } else if (testcase == "eg6") {
        Eg6Bench bench(period, size, 10, 20, size);
        time = bench.run();
    } else if (testcase == "eg7") {
        Eg7Bench bench(period, size, 10, 20, size);
        time = bench.run();
    } else {
        throw runtime_error("Invalid testcase");
    }

    cout << "Testcase: " << testcase <<", Size: " << size
        << ", Time: " << setprecision(3) << time / 1000000 << endl;

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
