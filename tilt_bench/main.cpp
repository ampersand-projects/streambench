#include <iostream>

#include "tilt_bench.h"

using namespace std;

int main(int argc, char** argv)
{
    MOCABench bench(1, 20, 50, 10, 100);
    auto time = bench.run();
    cout << "Time: " << time << endl;

    return 0;
}
