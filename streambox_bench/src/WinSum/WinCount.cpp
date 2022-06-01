#include "streambench/WinSum/WinCount.h"

#define MyWinSum WinCount
#include "WinSum/WinSum-dispatch-eval.h"

template
void
WinCount<yahoo_event_projected, uint64_t>::ExecEvaluator(int nodeid,
    EvaluationBundleContext *c, shared_ptr<BundleBase> bundle);