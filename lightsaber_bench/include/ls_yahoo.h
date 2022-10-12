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
        /* For some reason, it only works when I add padding to the schema to make it 64 bytes */
        auto schema = new TupleSchema(8, "YahooBenchmark");
        auto longAttr = AttributeType(BasicType::Long);

        schema->setAttributeType(0, longAttr); /*          st:  long  */
        schema->setAttributeType(1, longAttr); /*          dur: long  */
        schema->setAttributeType(2, longAttr); /*     user_id:  long  */
        schema->setAttributeType(3, longAttr); /* campaign_id:  long  */
        schema->setAttributeType(4, longAttr); /*  event_type:  long  */
        schema->setAttributeType(5, longAttr); /*  dummy:  long  */
        schema->setAttributeType(6, longAttr); /*  dummy:  long  */
        schema->setAttributeType(7, longAttr); /*  dummy:  long  */
        
        return schema;
    }

    void PopulateBufferWithData(int64_t size, int64_t period) override
    {
        // 10 input buffers for now
        for (size_t i = 0; i < 1000; i++) {
            auto buffer = new std::vector<char> (size * sizeof(YahooSchema));
            auto ptr = (YahooSchema *) buffer->data();
            for (unsigned long idx = 0; idx < size; idx++) {
                ptr[idx].st = i * 1000 + idx * period;
                ptr[idx].dur = period;
                ptr[idx].user_id =  static_cast<long>(rand() % 5 + 1);
                ptr[idx].campaign_id =  static_cast<long>(2);
                ptr[idx].event_type =  static_cast<long>(rand() % 5 + 1);
            }
            InputBuffers.push_back(buffer);
        }
    }

    void createApplication() override
    {
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

        // Must do a Groupby here, otherwise hit unhandled bug
        std::vector<Expression *> groupByAttributes(1);
        groupByAttributes[0] = new ColumnReference(3, BasicType::Long);

        auto window = new WindowDefinition(RANGE_BASED, window_size, window_size);
        Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

        bool replayTimestamps = window->isRangeBased();

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
        queries[0] = std::make_shared<Query>(0, operators, *window, getSchema(), timestampReference, true, replayTimestamps, !replayTimestamps);

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
        long dummy1;
        long dummy2;
        long dummy3;
    };
    
    YahooBench(long window_size)
        : window_size(window_size)
    {
        createApplication();
    }
};

#endif // LIGHTSABER_BENCH_INCLUDE_TEST_YAHOO_H_