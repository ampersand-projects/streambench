#include "streambench/WinSum/WinSum_addfloat.h"

template <>
float const & WinSum_addfloat<float, float>::aggregate(float * acc,
		float const & in) {
	*acc += in;
	return *acc;
}

template <>
float const & WinSum_addfloat<float, float>::aggregate_init(float * acc) {
	*acc = 0;
	return *acc;
}

template <>
float const & WinSum_addfloat<float, float>::combine
	(float & mine, float const & others) {
	mine += others;
	return mine;
}

template <>
temporal_event const & WinSum_addfloat<temporal_event, temporal_event>::aggregate(temporal_event * acc,
		temporal_event const & in) {
	acc->dur = in.dur;
	acc->payload += in.payload;
	return *acc;
}

template <>
temporal_event const & WinSum_addfloat<temporal_event, temporal_event>::aggregate_init(temporal_event * acc) {
	acc->dur = 0;
	acc->payload = 0;
	return *acc;
}

template <>
temporal_event const & WinSum_addfloat<temporal_event, temporal_event>::combine
	(temporal_event & mine, temporal_event const & others) {
	mine.dur = others.dur;
	mine.payload += others.payload;
	return mine;
}

#define MyWinSum WinSum_addfloat
#include "WinSum/WinSum-dispatch-eval.h"

template
void
WinSum_addfloat<float, float>
::ExecEvaluator(int nodeid,
                EvaluationBundleContext *c, shared_ptr<BundleBase> bundle);

template
void
WinSum_addfloat<temporal_event, temporal_event>
::ExecEvaluator(int nodeid,
                EvaluationBundleContext *c, shared_ptr<BundleBase> bundle);
