#ifndef LIGHTSABER_BENCH_INCLUDE_TEST_YAHOO_H_
#define LIGHTSABER_BENCH_INCLUDE_TEST_YAHOO_H_

#include "ls_base.h"

#include "cql/expressions/ColumnReference.h"
#include "cql/expressions/FloatConstant.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"


class YahooBench : public Benchmark
{
    long window_size;
    TupleSchema *getSchema () override
    {
        auto schema = new TupleSchema(5, "YahooBenchmark");
        auto longAttr = AttributeType(BasicType::Long);

        schema->setAttributeType(0, longAttr); /*          st:  long  */
        schema->setAttributeType(1, longAttr); /*          dur: long  */
        schema->setAttributeType(2, longAttr); /*     user_id:  long  */
        schema->setAttributeType(3, longAttr); /* campaign_id:  long  */
        schema->setAttributeType(4, longAttr); /*  event_type:  long  */
        
        return schema;
    }

    void PopulateBufferWithData(int64_t size, int64_t period) override
    {
        InputBuffer = new std::vector<char> (size * sizeof(YahooSchema));
        auto ptr = (YahooSchema *) InputBuffer->data();
        for (unsigned long idx = 0; idx < size; idx++) {
            ptr[idx].st = idx * period;
            ptr[idx].dur = period;
            ptr[idx].user_id =  static_cast<long>(rand() % 5 + 1);
            ptr[idx].campaign_id =  static_cast<long>(rand() % 5 + 1);
            ptr[idx].event_type =  static_cast<long>(rand() % 5 + 1);
        }
    }

    void createApplication() override
    {
        SystemConf::getInstance().SLOTS = 256;
        SystemConf::getInstance().PARTIAL_WINDOWS = 64;
        
        // Filter out event that has event_type == 1
        auto predicate = new ComparisonPredicate(EQUAL_OP, new ColumnReference(4, BasicType::Long), new LongConstant(1));
        Selection *selection = new Selection(predicate);

        // Only keep fields st, dur, campaign_id and event_type
        std::vector<Expression *> expressions(4);
        expressions[0] = new ColumnReference(0, BasicType::Long);
        expressions[1] = new ColumnReference(1, BasicType::Long);
        expressions[2] = new ColumnReference(3, BasicType::Long);
        expressions[3] = new ColumnReference(4, BasicType::Long);
        Projection *projection = new Projection(expressions);

        // Tumbling Window Count
        std::vector<AggregationType> aggregationTypes(1);
        aggregationTypes[0] = AggregationTypes::fromString("cnt");

        std::vector<ColumnReference *> aggregationAttributes(1);
        aggregationAttributes[0] = new ColumnReference(2, BasicType::Long);

        std::vector<Expression *> groupByAttributes(0);

        auto window = new WindowDefinition(ROW_BASED, window_size, window_size);
        Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

        // Set up code-generated operator
        OperatorKernel *genCode = new OperatorKernel(true);
        genCode->setInputSchema(getSchema());
        genCode->setSelection(selection);
        genCode->setProjection(projection);
        genCode->setAggregation(aggregation);
        genCode->setQueryId(0);
        genCode->setup();
        OperatorCode *cpuCode = genCode;

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
    struct alignas(16) YahooSchema {
        long st;
        long dur;
        long user_id;
        long campaign_id;
        long event_type;
    };
    
    YahooBench(long window_size)
        : window_size(window_size)
    {
        createApplication();
    }
};

#endif // LIGHTSABER_BENCH_INCLUDE_TEST_YAHOO_H_