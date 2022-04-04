#include "streambench/Mapper/ProjectMapperEvaluator.h"
#include "streambench/Mapper/ProjectMapper.h"

template <class InputT, class OutputT, template<class> class BundleT>
void ProjectMapper<InputT, OutputT, BundleT>::ExecEvaluator
	(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr)
{
	ProjectMapperEvaluator<InputT, OutputT, BundleT> eval(nodeid);
	eval.evaluate(this, c, bundle_ptr);
}

template<>
uint64_t ProjectMapper<temporal_event, temporal_event, RecordBundle>::do_map
	(Record<temporal_event> const &in, shared_ptr<RecordBundle<temporal_event>> output_bundle)
{
	temporal_event e {in.data.dur, in.data.payload};
	output_bundle->emplace_record(projector(e), in.ts);

	return 1;
}

template
void ProjectMapper<temporal_event, temporal_event, RecordBundle>::ExecEvaluator
	(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle = nullptr);