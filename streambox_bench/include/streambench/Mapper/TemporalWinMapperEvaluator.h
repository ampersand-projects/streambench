#ifndef TEMPORALWINMAPPEREVALUATOR_H_
#define TEMPORALWINMAPPEREVALUATOR_H_

#include "core/SingleInputTransformEvaluator.h"

#include "streambench/Mapper/TemporalWinMapper.h"

template <typename InputT, typename OutputT, template<class> class BundleT_>
class TemporalWinMapperEvaluator
	: public SingleInputTransformEvaluator<TemporalWinMapper<InputT,OutputT,BundleT_>,
	  BundleT_<InputT>, WindowsBundle<OutputT>> {

	using TransformT = TemporalWinMapper<InputT,OutputT,BundleT_>;
	using InputBundleT = BundleT_<InputT>;
	using OutputBundleT = WindowsBundle<OutputT>; /* always produce WindowsBundle */

public:

	TemporalWinMapperEvaluator(int node) :
			SingleInputTransformEvaluator<TransformT, InputBundleT, OutputBundleT>(
					node) {
	}

	bool evaluateSingleInput(TransformT* trans,
			shared_ptr<InputBundleT> input_bundle,
			shared_ptr<OutputBundleT> output_bundle) override {

		uint64_t cnt = 0;

		for (auto && it = input_bundle->begin(); it != input_bundle->end(); ++it) {
			cnt += trans->do_map(*it, output_bundle);
		}

		if (cnt)
			return true;
		else
			return false;
	}
};

#endif /* TEMPORALWINMAPPEREVALUATOR_H_ */
