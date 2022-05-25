#ifndef PROJECT_MAPPER_H
#define PROJECT_MAPPER_H

#include "Values.h"
#include "Mapper/Mapper.h"

using namespace std;

template <class InputT, class OutputT, template<class> class BundleT>
class ProjectMapper : public Mapper<InputT> {
	using InputBundleT = BundleT<InputT>;
	using OutputBundleT = BundleT<OutputT>;

private:
	function<OutputT(InputT)> projector;

public:
  	ProjectMapper(string name, function<OutputT(InputT)> projector) :
	  	Mapper<InputT>(name),
		projector(projector)
	{}

  	uint64_t do_map(Record<InputT> const& in, shared_ptr<OutputBundleT> output_bundle)
	{
		output_bundle->emplace_record(projector(in.data), in.ts);
		return 1;
	}

  	void ExecEvaluator(int, EvaluationBundleContext*, shared_ptr<BundleBase> = nullptr) override;
};




#endif // PROJECT_MAPPER_H
