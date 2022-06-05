#include "streambench/Mapper/ProjectMapperEvaluator.h"
#include "streambench/Mapper/ProjectMapper.h"

template <class InputT, class OutputT, template<class> class BundleT>
void ProjectMapper<InputT, OutputT, BundleT>::ExecEvaluator
	(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr)
{
	ProjectMapperEvaluator<InputT, OutputT, BundleT> eval(nodeid);
	eval.evaluate(this, c, bundle_ptr);
}

template
void ProjectMapper<temporal_event, temporal_event, RecordBundle>::ExecEvaluator
	(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle = nullptr);

template
void ProjectMapper<yahoo_event, yahoo_event_projected, RecordBundle>::ExecEvaluator
	(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle = nullptr);