#ifndef FILTER_MAPPER_H
#define FILTER_MAPPER_H

#include "Values.h"
#include "Mapper/Mapper.h"

using namespace std;

template <class InputT, class OutputT, template<class> class BundleT>
class FilterMapper : public Mapper<InputT> {
	using InputBundleT = BundleT<InputT>;
	using OutputBundleT = BundleT<OutputT>;

private:
	function<bool(temporal_event)> filter;

public:
  	FilterMapper(string name, function<bool(temporal_event)> filter) :
	  	Mapper<InputT>(name),
		filter(filter)
	{}

  	uint64_t do_map(Record<InputT> const&, shared_ptr<OutputBundleT>);
  	void ExecEvaluator(int, EvaluationBundleContext*, shared_ptr<BundleBase> = nullptr) override;
};

#endif // FILTER_MAPPER_H
