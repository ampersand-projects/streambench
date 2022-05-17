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

#define MyWinSum WinSum_addfloat
#include "WinSum/WinSum-dispatch-eval.h"

template
void
WinSum_addfloat<float, float>
::ExecEvaluator(int nodeid,
                EvaluationBundleContext *c, shared_ptr<BundleBase> bundle);
