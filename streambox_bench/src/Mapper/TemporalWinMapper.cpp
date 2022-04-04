#include "streambench/Mapper/TemporalWinMapper.h"
#include "streambench/Mapper/TemporalWinMapperEvaluator.h"

template<class InputT, class OutputT, template<class> class BundleT_>
void TemporalWinMapper<InputT, OutputT, BundleT_>::ExecEvaluator(int nodeid,
		EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr)
{
	/* instantiate an evaluator */
	TemporalWinMapperEvaluator<InputT, OutputT, BundleT_> eval(nodeid);
	eval.evaluate(this, c, bundle_ptr);
}

template<>
uint64_t TemporalWinMapper<temporal_event, long, RecordBundle>::do_map
	(Record<temporal_event> const &in, shared_ptr<WindowsBundle<long>> output_bundle)
{
	long offset = (in.ts - this->start).total_microseconds() \
        % (this->window_size).total_microseconds();
    output_bundle->add_value(
		Window(in.ts - microseconds(offset), this->window_size),
		static_cast<long>(in.data.payload)
	);

    return 1;
}

/* -------instantiation concrete classes------- */
template
void TemporalWinMapper<temporal_event, long, RecordBundle>::ExecEvaluator(int nodeid,
		EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);
