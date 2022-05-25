#ifndef FILTER_MAPPER_H
#define FILTER_MAPPER_H

#include "Values.h"
#include "Mapper/Mapper.h"

using namespace std;

template <class T, template<class> class BundleT>
class FilterMapper : public Mapper<T> {
	using InputBundleT = BundleT<T>;
	using OutputBundleT = BundleT<T>;

private:
	function<bool(T)> filter;

public:
  	FilterMapper(string name, function<bool(T)> filter) :
	  	Mapper<T>(name),
		filter(filter)
	{}

  	uint64_t do_map(Record<T> const& in, shared_ptr<OutputBundleT> output_bundle)
	{
		if (filter(in.data)) {
			output_bundle->emplace_record(in.data, in.ts);
		}

		return 1;
	}

  	void ExecEvaluator(int, EvaluationBundleContext*, shared_ptr<BundleBase> = nullptr) override;
};

#endif // FILTER_MAPPER_H
