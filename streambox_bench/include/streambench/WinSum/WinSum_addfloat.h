
#ifndef WINDOWEDSUMADDFLOAT_H
#define WINDOWEDSUMADDFLOAT_H

#include "WinSum/WinSumBase.h"

template <typename InputT, typename OutputT>
class WinSum_addfloat : public WinSumBase<WinSum_addfloat, InputT, OutputT> {

public:

	WinSum_addfloat(string name, int multi = 1)
		: WinSumBase<WinSum_addfloat, InputT, OutputT>(name, multi) { }
	
  /* for aggregating one single input. to be specialized */
  static OutputT const & aggregate_init(OutputT * acc);
  static OutputT const & aggregate(OutputT * acc, InputT const & in);

  /* combine the evaluator's (partial) aggregation results (for a particular window)
   * to the tran's internal state.
   */
  static OutputT const & combine(OutputT & mine, OutputT const & others);

	void ExecEvaluator(int nodeid, EvaluationBundleContext *c,
    std::shared_ptr<BundleBase> bundle) override;
};

#endif /* WINDOWEDSUMADDFLOAT_H */

