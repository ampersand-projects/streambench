#ifndef LIGHTSABER_BENCH_INCLUDE_TEST_PROJECTION_H_
#define LIGHTSABER_BENCH_INCLUDE_TEST_PROJECTION_H_

#include "ls_base.h"

#include "cql/expressions/ColumnReference.h"
#include "cql/expressions/FloatConstant.h"
#include "cql/expressions/operations/Addition.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"


class SelectBench : public Benchmark
{
    long window_size;
    void createApplication() override
    {
        // Configure projection
        std::vector<Expression *> expressions(3);
        // Always project the timestamp
        expressions[0] = new ColumnReference(0, BasicType::Long);
        expressions[1] = new ColumnReference(1, BasicType::Long);
        expressions[2] = new Addition(new ColumnReference(2, BasicType::Float), new FloatConstant(3), BasicType::Float);
        Projection *projection = new Projection(expressions);

        auto window = new WindowDefinition(ROW_BASED, window_size, window_size);

        // Set up code-generated operator
        OperatorKernel *genCode = new OperatorKernel(true);
        genCode->setInputSchema(getSchema());
        genCode->setProjection(projection);
        genCode->setQueryId(0);
        genCode->setup();
        OperatorCode *cpuCode = genCode;

        auto queryOperator = new QueryOperator(*cpuCode);
        std::vector<QueryOperator *> operators;
        operators.push_back(queryOperator);

        long timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

        std::vector<std::shared_ptr<Query>> queries(1);
        queries[0] = std::make_shared<Query>(0, operators, *window, getSchema(), timestampReference, false, false, true);

        application = new QueryApplication(queries);
        application->setup();
    }

    public:
    SelectBench(long window_size)
        : window_size(window_size)
    {
        createApplication();
    }
};

#endif // LIGHTSABER_BENCH_INCLUDE_TEST_PROJECTION_H_