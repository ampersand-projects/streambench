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

struct __attribute__((packed)) yahooData
{
  long start_time;
  long end_time;
  long userID;
  long campaignID;
  long event_type;
  yahooData() {}
};

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cout << "1. argument: Number of tuples. 2. Period"
              << std::endl;
    return -1;
  }

  size_t size = atoi(argv[1]);
  size_t period = atoi(argv[2]);

  yahooData *recs = new yahooData[size];

  for (size_t i = 0; i < size; i++) {
    recs[i].start_time = i * period;
    recs[i].end_time = (i + 1) * period;
    recs[i].userID = rand() % 5 + 1;
    recs[i].campaignID = rand() % 5 + 1;
    recs[i].event_type = rand() % 5 + 1;
  }

  std::ofstream ofp("yahoo_data.bin", std::ios::out | std::ios::binary);
  ofp.write(reinterpret_cast<const char *>(recs), size * sizeof(yahooData));
  ofp.close();
}
