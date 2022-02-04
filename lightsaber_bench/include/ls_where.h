#ifndef LIGHTSABER_BENCH_INCLUDE_TEST_SELECTION_H_
#define LIGHTSABER_BENCH_INCLUDE_TEST_SELECTION_H_

#include "ls_base.h"

#include "cql/expressions/ColumnReference.h"
#include "cql/expressions/FloatConstant.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"


class WhereBench : public Benchmark {
    long window_size;
    void createApplication() override {
        auto predicate = new ComparisonPredicate(GREATER_OP, new ColumnReference(2, BasicType::Float), new FloatConstant(0));
        Selection *selection = new Selection(predicate);

        auto window = new WindowDefinition(ROW_BASED, window_size, window_size);

        // Set up code-generated operator
        OperatorKernel *genCode = new OperatorKernel(true);
        genCode->setInputSchema(getSchema());
        genCode->setSelection(selection);
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
    WhereBench(long window_size) : window_size(window_size){
        createApplication();
    }
};

#endif // LIGHTSABER_BENCH_INCLUDE_TEST_SELECTION_H_