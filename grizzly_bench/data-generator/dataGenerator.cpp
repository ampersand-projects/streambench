#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <stdio.h>
#include <string>
#include <stdlib.h>

typedef uint64_t Timestamp;
using NanoSeconds = std::chrono::nanoseconds;
using Clock = std::chrono::high_resolution_clock;

struct __attribute__((packed)) data
{
  uint64_t start_time;
  uint64_t end_time;
  double payload;
  data() {}
};

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cout << "1. argument: Number of tuples. 2. Period"
              << std::endl;
    return -1;
  }

  size_t processCnt = atoi(argv[1]);
  size_t period = atoi(argv[2]);

  data *recs = new data[processCnt];

  double range = 100.0;
  for (size_t i = 0; i < processCnt; i++) {
    recs[i].start_time = i * period;
    recs[i].end_time = (i + 1) * period;
    recs[i].payload = ((double)rand() / (RAND_MAX)) * range - range / 2;
  }

  std::ofstream ofp("test_data.bin", std::ios::out | std::ios::binary);
  ofp.write(reinterpret_cast<const char *>(recs), processCnt * sizeof(data));
  ofp.close();
}
