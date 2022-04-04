#include "streambench/Mapper/FilterMapperEvaluator.h"
#include "streambench/Mapper/FilterMapper.h"

template <class InputT, class OutputT, template<class> class BundleT>
void FilterMapper<InputT, OutputT, BundleT>::ExecEvaluator
	(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr)
{
	FilterMapperEvaluator<InputT, OutputT, BundleT> eval(nodeid);
	eval.evaluate(this, c, bundle_ptr);
}

template<>
uint64_t FilterMapper<temporal_event, temporal_event, RecordBundle>::do_map
	(Record<temporal_event> const &in, shared_ptr<RecordBundle<temporal_event>> output_bundle)
{
	if (filter(in.data)) {
		temporal_event e {in.data.dur, in.data.payload + 1};
		output_bundle->emplace_record(e, in.ts);
	}

	return 1;
}

template
void FilterMapper<temporal_event, temporal_event, RecordBundle>::ExecEvaluator
	(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle = nullptr);