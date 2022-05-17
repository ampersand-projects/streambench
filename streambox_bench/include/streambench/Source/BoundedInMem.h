#ifndef BOUNDEDINMEM_H_
#define BOUNDEDINMEM_H_

#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include <numa.h>
#include <cstdlib>

#include <boost/progress.hpp> /* progress bar */

#include "config.h"
#include "log.h"

#include "Source/Bounded.h"
#include "core/Transforms.h"

using namespace std;

template<class T, template<class> class BundleT>
class BoundedInMem : public PTransform {
public:
	int64_t dur;
	vector<Record<T> *> record_buffers;
	uint64_t buffer_size_records = 0;
	const unsigned long records_total;
	const unsigned long records_per_interval;

  	ptime base_ts = ptime(boost::gregorian::date(1970, Jan, 1));

public:
  	BoundedInMem (string name, int64_t dur,
  		unsigned long records_total, unsigned long rpi) :
	PTransform(name), dur(dur),
	records_total(records_total),
    records_per_interval(rpi)
  	{
		int num_nodes = numa_num_configured_nodes();

		buffer_size_records = records_total;
		xzl_assert(buffer_size_records > 0);

		/* fill the buffers of records */
		for (int i = 0; i < num_nodes; i++) {
			Record<T> * record_buffer = (Record<T> *) numa_alloc_onnode(sizeof(Record<T>) * buffer_size_records, i);
			xzl_assert(record_buffer);

			for (unsigned int j = 0; j < buffer_size_records; j++) {
				fill_record_buffer(record_buffer, j);
				record_buffer[j].ts = base_ts + boost::posix_time::milliseconds(j);
			}

			record_buffers.push_back(record_buffer);
		}
  	}

	void fill_record_buffer(Record<temporal_event> *record_buffer, unsigned int j) {
		double range = 100;
		record_buffer[j].data.dur = dur;
		record_buffer[j].data.payload = static_cast<float>(rand() / static_cast<double>(RAND_MAX / range)) - (range / 2);
	}

	void fill_record_buffer(Record<long> *record_buffer, unsigned int j) {
		record_buffer[j].data = static_cast<long> (j);
	}

	virtual ptime RefreshWatermark(ptime wm) override {
		return wm;
	}

  	void ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase>) override;
};

#endif /* BOUNDEDINMEM_H_ */