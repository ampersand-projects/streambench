#ifndef TEMPORALWINMAPPER_H_
#define TEMPORALWINMAPPER_H_

#include "Values.h"
#include "Mapper/Mapper.h"
#include "boost/date_time/posix_time/ptime.hpp"

template <class InputT, class OutputT, template<class> class BundleT>
class TemporalWinMapper : public Mapper<InputT> {

	using InputBundleT = BundleT<InputT>;
	using OutputBundleT = WindowsBundle<OutputT>;

public:
  const boost::posix_time::time_duration window_size;
  const ptime start; // the starting point of windowing.

public:
	TemporalWinMapper(string name,
      	boost::posix_time::time_duration window_size,
     	ptime start = Window::epoch) :  
	Mapper<InputT>(name), window_size(window_size), start(start) {}


	void ExecEvaluator(int nodeid, EvaluationBundleContext *c,
	  		shared_ptr<BundleBase> bundle_ptr = nullptr) override;
  	uint64_t do_map(Record<InputT> const & in, shared_ptr<OutputBundleT> output_bundle);
};

#endif /* TEMPORALWINMAPPER_H_ */
