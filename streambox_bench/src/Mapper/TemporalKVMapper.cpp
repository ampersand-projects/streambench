#include "streambench/Mapper/TemporalKVMapper.h"
#include "streambench/Mapper/TemporalKVMapperEvaluator.h"

template<class InputT, class OutputT, template<class> class BundleT_>
void TemporalKVMapper<InputT, OutputT, BundleT_>::ExecEvaluator(int nodeid,
		EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr)
{
	/* instantiate an evaluator */
	TemporalKVMapperEvaluator<InputT, OutputT, BundleT_> eval(nodeid);
	eval.evaluate(this, c, bundle_ptr);
}

template<>
uint64_t TemporalKVMapper<temporal_event, pair<long, long>, RecordBundle>::do_map
	(Record<temporal_event> const &in, shared_ptr<RecordBundle<pair<long, long>>> output_bundle)
{
	auto diff = in.ts - base_ts;
    long st = diff.total_milliseconds();
	pair<long, long> kv_pair (st, static_cast<long>(in.data.payload));
	output_bundle->emplace_record(kv_pair, in.ts);

	return 1;
}

/* -------instantiation concrete classes------- */
template
void TemporalKVMapper<temporal_event, pair<long, long>, RecordBundle>::ExecEvaluator(int nodeid,
		EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);
