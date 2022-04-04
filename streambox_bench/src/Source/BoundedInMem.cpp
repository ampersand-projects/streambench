#include "Values.h"
#include "streambench/Source/BoundedInMemEvaluator.h"
#include "streambench/Source/BoundedInMem.h"

template<template<class> class BundleT>
void BoundedInMem<temporal_event, BundleT>::ExecEvaluator(int nodeid,
		EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr) {
	/* instantiate an evaluator */
	BoundedInMemEvaluator<temporal_event, BundleT> eval(nodeid);
	eval.evaluate(this, c, bundle_ptr);
}

template
void BoundedInMem<temporal_event, RecordBundle>
		::ExecEvaluator(int nodeid,
			EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr);
