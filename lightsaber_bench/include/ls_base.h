#ifndef LIGHTSABER_BENCH_INCLUDE_BENCHMARK_H_
#define LIGHTSABER_BENCH_INCLUDE_BENCHMARK_H_

#include <netinet/in.h>
#include <sys/socket.h>
#include <chrono>

#include <utils/SystemConf.h>
#include "utils/TupleSchema.h"
#include "utils/QueryApplication.h"
#include "utils/QueryOperator.h"
#include "utils/Query.h"
#include "utils/WindowDefinition.h"

class Benchmark {
    public:
    struct alignas(16) InputSchema {
        long st;
        long dur;
        float payload;
    };
    int m_sock = 0;
    int m_server_fd;
    std::vector<char> *InputBuffer = nullptr;
    QueryApplication *application = nullptr;
    virtual void createApplication() = 0;

    TupleSchema *getSchema() {
        auto schema = new TupleSchema(3, "Micro-Benchmark");
        auto longAttr = AttributeType(BasicType::Long);
        auto floatAttr = AttributeType(BasicType::Float);

        schema->setAttributeType(0, longAttr);
        schema->setAttributeType(1, longAttr);
        schema->setAttributeType(2, floatAttr);

        return schema;
    }

    void PopulateBufferWithData(int64_t size, int64_t period) {
        InputBuffer = new std::vector<char> (size * sizeof(InputSchema));
        auto ptr = (InputSchema *) InputBuffer->data();
        for (unsigned long idx = 0; idx < size; idx++) {
            ptr[idx].st = idx * period;
            ptr[idx].dur = 1L;
            ptr[idx].payload = 1.5f;
        }
    }

    int64_t runBenchmark(int64_t size, int64_t period) {
        PopulateBufferWithData(size, period);

        auto start_time = std::chrono::high_resolution_clock::now();
        application->processData(*InputBuffer, -1);
        auto end_time = std::chrono::high_resolution_clock::now();
        
        int64_t time = duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        return time;
    }

};

#endif // LIGHTSABER_BENCH_INCLUDE_BENCHMARK_H_