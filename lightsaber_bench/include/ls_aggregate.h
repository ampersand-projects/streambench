#ifndef LIGHTSABER_BENCH_INCLUDE_TEST_AGGREGATE_H_
#define LIGHTSABER_BENCH_INCLUDE_TEST_AGGREGATE_H_

#include "ls_base.h"

#include "cql/expressions/ColumnReference.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"


class AggregateBench : public Benchmark
{
    long window_size;
    void createApplication() override
    {
        // Configure non-grouped aggregation. Check the application benchmarks for grouped aggreagations.
        std::vector<AggregationType> aggregationTypes(1);
        aggregationTypes[0] = AggregationTypes::fromString("sum");

        std::vector<ColumnReference *> aggregationAttributes(1);
        aggregationAttributes[0] = new ColumnReference(2, BasicType::Float);

        std::vector<Expression *> groupByAttributes(0);

        auto window = new WindowDefinition(ROW_BASED, window_size, window_size);
        Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

        // Set up code-generated operator
        OperatorKernel *genCode = new OperatorKernel(true);
        genCode->setInputSchema(getSchema());
        genCode->setAggregation(aggregation);
        genCode->setQueryId(0);
        genCode->setup();
        OperatorCode *cpuCode = genCode;

        // Print operator
        std::cout << cpuCode->toSExpr() << std::endl;

        auto queryOperator = new QueryOperator(*cpuCode);
        std::vector<QueryOperator *> operators;
        operators.push_back(queryOperator);

        long timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

        std::vector<std::shared_ptr<Query>> queries(1);
        queries[0] = std::make_shared<Query>(0, operators, *window, getSchema(), timestampReference, true, false, true);

        application = new QueryApplication(queries);
        application->setup();
    }

    public:
    AggregateBench(long window_size)
        : window_size(window_size)
    {
        createApplication();
    }
};

#endif // LIGHTSABER_BENCH_INCLUDE_TEST_AGGREGATE_H_