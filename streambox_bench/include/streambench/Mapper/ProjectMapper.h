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
	function<temporal_event(temporal_event)> projector;

public:
  	ProjectMapper(string name, function<temporal_event(temporal_event)> projector) :
	  	Mapper<InputT>(name),
		projector(projector)
	{}

  	uint64_t do_map(Record<InputT> const&, shared_ptr<OutputBundleT>);
  	void ExecEvaluator(int, EvaluationBundleContext*, shared_ptr<BundleBase> = nullptr) override;
};




#endif // PROJECT_MAPPER_H
