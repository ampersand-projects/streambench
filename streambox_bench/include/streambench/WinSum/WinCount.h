
#ifndef WINDOWEDCOUNT_H
#define WINDOWEDCOUNT_H

#include "WinSum/WinSumBase.h"

template <typename InputT, typename OutputT>
class WinCount : public WinSumBase<WinCount, InputT, OutputT> {
public:
	WinCount(string name, int multi = 1) :
        WinSumBase<WinCount, InputT, OutputT>(name, multi)
    {}
	
    static OutputT const & aggregate_init(OutputT * acc)
    {
        *acc = 0;
        return *acc;
    }

    static OutputT const & aggregate(OutputT * acc, InputT const & in)
    {
        *acc += 1;
	    return *acc;
    }

    static OutputT const & combine(OutputT & mine, OutputT const & others) {
        mine += others;
        return mine;
    }

	void ExecEvaluator(int nodeid, EvaluationBundleContext *c,
    std::shared_ptr<BundleBase> bundle) override;
};

#endif /* WINDOWEDCOUNT_H */