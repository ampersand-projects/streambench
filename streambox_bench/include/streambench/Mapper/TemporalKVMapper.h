#ifndef KV_MAPPER_H
#define KV_MAPPER_H

#include "Values.h"
#include "Mapper/Mapper.h"

using namespace std;

template <class InputT, class OutputT, template<class> class BundleT>
class TemporalKVMapper : public Mapper<InputT> {
	using InputBundleT = BundleT<InputT>;
	using OutputBundleT = BundleT<OutputT>;

private:
	ptime base_ts = ptime(boost::gregorian::date(1970, Jan, 1));

public:
  	TemporalKVMapper(string name) :
	  	Mapper<InputT>(name)
	{}

  	uint64_t do_map(Record<InputT> const&, shared_ptr<OutputBundleT>);
  	void ExecEvaluator(int, EvaluationBundleContext*, shared_ptr<BundleBase> = nullptr) override;
};

#endif // KV_MAPPER_H
