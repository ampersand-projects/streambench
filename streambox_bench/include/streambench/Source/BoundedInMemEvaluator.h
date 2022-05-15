#ifndef BOUNDEDINMEMEVALUATOR_H
#define BOUNDEDINMEMEVALUATOR_H

extern "C" {
#include "measure.h"
}

#include "core/TransformEvaluator.h"
#include "core/EvaluationBundleContext.h"

#include "streambench/Source/BoundedInMem.h"

using namespace Kaskade;

template<class T, template<class> class BundleT>
class BoundedInMemEvaluator
	: public TransformEvaulator<BoundedInMem<T, BundleT>> {
	using TransformT = BoundedInMem<T, BundleT>;

#ifdef USE_NUMA_TP
	using TicketT = Ticket;
#else
	using TicketT = std::future<void>;
#endif

public:
	BoundedInMemEvaluator(int node) {}

	static void SeedSourceWatermarkBlocking(EvaluationBundleContext *c,
			shared_ptr<vector<TicketT>> tickets,
			const ptime wm)
	{

		for (auto && t : *tickets)
			t.wait();

		/* propagate source's watermark snapshot downstream. */
		c->PropagateSourceWatermarkSnapshot(wm);
	}

	/* tp version; executed in a separate thread; expecting a thread id */
	static void SeedSourceWatermarkBlockingTP(int id, EvaluationBundleContext *c,
			shared_ptr<vector<TicketT>> tickets,
			const ptime ts)
	{
		//  	EE("watermark: %s...", to_simple_string(ts).c_str());

		//  	EE("%s: wm: %s...",
		//  			to_simple_string(boost::posix_time::microsec_clock::local_time()).c_str(),
		//  			to_simple_string(ts).c_str());

		SeedSourceWatermarkBlocking(c, tickets, ts);
	}

	private:
	/* moved from WindowKeyedReducerEval (XXX merge later)
	 * task_id: zero based.
	 * return: <start, cnt> */
	static pair<int,int> get_range(int num_items, int num_tasks, int task_id) {
		/* not impl yet */
		xzl_bug_on(num_items == 0);

		xzl_bug_on(task_id > num_tasks - 1);

		int items_per_task  = num_items / num_tasks;

		/* give first @num_items each 1 item */
		if (num_items < num_tasks) {
			if (task_id <= num_items - 1)
				return make_pair(task_id, 1);
			else
				return make_pair(0, 0);
		}

		/* task 0..n-2 has items_per_task items. */
		if (task_id != num_tasks - 1)
			return make_pair(task_id * items_per_task, items_per_task);

		if (task_id == num_tasks - 1) {
			int nitems = num_items - task_id * items_per_task;
			xzl_bug_on(nitems < 0);
			return make_pair(task_id * items_per_task, nitems);
		}

		xzl_bug("bug. never reach here");
		return make_pair(-1, -1);
	}

	public:
	void evaluate(TransformT* t, EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr = nullptr) override
	{
		PValue* out[] = { t->getFirstOutput(), t->getSecondOutput() };
		assert(out[0]);

		int num_outputs = 1;
		if (out[1])
			num_outputs = 2;

		/* # of bundles between two puncs */
		const uint64_t bundle_per_interval = 1 * c->num_workers; /* # cores */
		const uint64_t records_per_bundle = t->records_per_interval / bundle_per_interval;

		ptime current_ts = t->base_ts;

		const int num_nodes = numa_max_node() + 1;

		uint64_t offset = 0;
		const int total_tasks = CONFIG_SOURCE_THREADS;

		vector <std::future<void>> futures;

		while (offset < t->buffer_size_records) {
			for (int task_id = 0; task_id < total_tasks; task_id++) {

				auto source_task_lambda = [t, &total_tasks, &bundle_per_interval,
				     this, &out, &c, &records_per_bundle, &num_nodes,
				     task_id, offset, num_outputs](int id)
				    {
					    auto range = get_range(bundle_per_interval, total_tasks, task_id);

					    auto local_offset = (offset + records_per_bundle * range.first) % t->buffer_size_records;
						uint64_t local_offsets[10] = {local_offset}; //support 10 input streams at most

					    for (int i = range.first; i < range.first + range.second; i++) {
						    
						    int nodeid = (i % num_nodes);

							for(int oid = 0; oid < num_outputs; oid++) {

								shared_ptr<BundleT<T>>
									bundle(make_shared<BundleT<T>>(
											records_per_bundle,  /* reserved capacity */
											nodeid));

								xzl_assert(bundle);

								for (unsigned int j = 0; j < records_per_bundle; j++, local_offsets[oid]++) {
									if (local_offsets[oid] >= t->buffer_size_records) {
										break;
									}
									bundle->add_record(t->record_buffers[nodeid][local_offsets[oid]]);
								}

								out[oid]->consumer->depositOneBundle(bundle, nodeid);
								c->SpawnConsumer();
							}
					    }
				    };

				/* exec the N-1 task inline */
				if (task_id == total_tasks - 1) {
					source_task_lambda(0);
					continue;
				}

				futures.push_back( // store a future
					c->executor_.push(source_task_lambda) /* submit to task queue */
				);
			}  // end of tasks

			for (auto && f : futures) {
				f.get();
			}
			futures.clear();

			/* advance the global record offset */
			offset += records_per_bundle * bundle_per_interval;
			current_ts += boost::posix_time::milliseconds(records_per_bundle * bundle_per_interval * t->dur); 

			c->UpdateSourceWatermark(current_ts);

			/* Useful before the sink sees the 1st watermark */
			if (c->GetTargetWm() == max_date_time) { /* unassigned */
				c->SetTargetWm(current_ts);
			}

			static int wm_node = 0;

			for(int oid = 0; oid < num_outputs; oid++) {
				out[oid]->consumer->depositOnePunc(make_shared<Punc>(current_ts, wm_node),
					wm_node);
			}
			c->SpawnConsumer();
			if (++wm_node == numa_max_node())
				wm_node = 0;
		}
	}
};

#endif /* BOUNDEDINMEMEVALUATOR_H */
