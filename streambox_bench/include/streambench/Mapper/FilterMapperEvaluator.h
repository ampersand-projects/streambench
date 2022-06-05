#ifndef FILTER_MAPPER_EVAL_H
#define FILTER_MAPPER_EVAL_H

#include "Values.h"
#include "core/SingleInputTransformEvaluator.h"

#include "streambench/Mapper/FilterMapper.h"

template <typename T, template<class> class BundleT>
	class FilterMapperEvaluator : 
		public SingleInputTransformEvaluator<
	   		FilterMapper<T, BundleT>, BundleT<T>, BundleT<T>
		>
{
	using InputBundleT = BundleT<T>;
	using OutputBundleT = BundleT<T>;
	using TransformT = FilterMapper<T, BundleT>;

public:
	bool evaluateSingleInput (TransformT* trans, shared_ptr<InputBundleT> input_bundle,
			shared_ptr<OutputBundleT> output_bundle) override
	{
		for (auto && it = input_bundle->begin(); it != input_bundle->end(); ++it) {
			trans->do_map(*it, output_bundle);
		}
		return true;
	}

	FilterMapperEvaluator(int node) :
		SingleInputTransformEvaluator<TransformT, InputBundleT, OutputBundleT>(node)
	{}
};

#endif // FILTER_MAPPER_EVAL_H
