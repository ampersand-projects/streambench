#include "Values.h"
#include "streambench/Source/BoundedInMemEvaluator.h"
#include "streambench/Source/BoundedInMem.h"

template<class T, template<class> class BundleT>
void BoundedInMem<T, BundleT>::ExecEvaluator(int nodeid,
		EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr) {
	/* instantiate an evaluator */
	BoundedInMemEvaluator<T, BundleT> eval(nodeid);
	eval.evaluate(this, c, bundle_ptr);
}

template
void BoundedInMem<temporal_event, RecordBundle>
		::ExecEvaluator(int nodeid,
			EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr);

template
void BoundedInMem<long, RecordBundle>
		::ExecEvaluator(int nodeid,
			EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr);
