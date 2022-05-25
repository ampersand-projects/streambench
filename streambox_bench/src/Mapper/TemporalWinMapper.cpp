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
uint64_t TemporalWinMapper<yahoo_event_projected, yahoo_event_projected, RecordBundle>::do_map
	(Record<yahoo_event_projected> const &in, shared_ptr<WindowsBundle<yahoo_event_projected>> output_bundle)
{
	long offset = (in.ts - this->start).total_microseconds() \
        % (this->window_size).total_microseconds();
    output_bundle->add_value(
		Window(in.ts - microseconds(offset), this->window_size),
		in.data
	);

    return 1;
}

template<>
uint64_t TemporalWinMapper<temporal_event, temporal_event, RecordBundle>::do_map
	(Record<temporal_event> const &in, shared_ptr<WindowsBundle<temporal_event>> output_bundle)
{
	long offset = (in.ts - this->start).total_microseconds() \
        % (this->window_size).total_microseconds();
	temporal_event event {
		static_cast<uint64_t>(this->window_size.total_milliseconds()),
		in.data.payload 
	};
    output_bundle->add_value(
		Window(in.ts - microseconds(offset), this->window_size),
		event
	);

    return 1;
}

/* -------instantiation concrete classes------- */
template
void TemporalWinMapper<temporal_event, temporal_event, RecordBundle>::ExecEvaluator(int nodeid,
		EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);

template
void TemporalWinMapper<yahoo_event_projected, yahoo_event_projected, RecordBundle>::ExecEvaluator(int nodeid,
		EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);
