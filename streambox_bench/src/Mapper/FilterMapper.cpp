#include "streambench/Mapper/FilterMapperEvaluator.h"
#include "streambench/Mapper/FilterMapper.h"

template <class T, template<class> class BundleT>
void FilterMapper<T, BundleT>::ExecEvaluator
	(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr)
{
	FilterMapperEvaluator<T, BundleT> eval(nodeid);
	eval.evaluate(this, c, bundle_ptr);
}

template
void FilterMapper<temporal_event, RecordBundle>::ExecEvaluator
	(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle = nullptr);

template
void FilterMapper<yahoo_event, RecordBundle>::ExecEvaluator
	(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle = nullptr);