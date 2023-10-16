#include "BenchmarkHelper.hpp"
#include "Defs.hpp"
#include "PerfEvent.hpp"
#include "nam/Compute.hpp"
#include "nam/Config.hpp"
#include "nam/Storage.hpp"
#include "nam/profiling/ProfilingThread.hpp"
#include "nam/profiling/counters/WorkerCounters.hpp"
#include "nam/threads/Concurrency.hpp"
#include "nam/utils/RandomGenerator.hpp"
#include "nam/utils/ScrambledZipfGenerator.hpp"
#include "nam/utils/Time.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
// -------------------------------------------------------------------------------------

DEFINE_double(run_for_seconds, 10.0, "");
DEFINE_uint64(lock_count, 16, "");
DEFINE_uint64(compute_id,0,"");


int main(int argc, char* argv[]) {
   gflags::SetUsageMessage("Storage-DB Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   using namespace nam;

   if (FLAGS_storage_node) {
      std::cout << "Storage Node" << std::endl;
      nam::Storage db;
      db.registerMemoryRegion("block", FLAGS_dramGB * 1024 * 1024 * 1024);
      db.startAndConnect();
      // -------------------------------------------------------------------------------------
      while (db.getCM().getNumberIncomingConnections()) {}
   } else {
      nam::Compute compute;
      std::string benchmark = "read";
      // -------------------------------------------------------------------------------------
      std::atomic<bool> keep_running = true;
      std::atomic<uint64_t> running_threads_counter = 0;
      uint64_t stage = 1;
      benchmark::ALIGNMENT_workloadInfo experimentInfo{benchmark, 0, 0};
      compute.startProfiler(experimentInfo);
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         compute.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            auto& cm = compute.getCM();
            auto* buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(1024, 64));
            uint64_t* barrier_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
            uint64_t* old = reinterpret_cast<uint64_t*>(buffer);
            auto& cctxs = threads::Worker::my().cctxs;
            auto& catalog = threads::Worker::my().catalog;
            auto barrier_addr = catalog[0].start;
            rdma_barrier_wait(barrier_addr, stage, barrier_buffer, *cctxs[0].rctx);

            auto poll_cq = [&](rdma::RdmaContext*& rctx) {
               int comp{0};
               ibv_wc wcReturn;
               while (comp == 0) {
                  _mm_pause();
                  comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
               }
            };
            const uint64_t s_id  = 0;
            running_threads_counter++;
            while (keep_running) {
               auto* rctx = cctxs[s_id].rctx;
               auto desc = catalog[s_id];
               uint64_t thread_id = (FLAGS_compute_id * FLAGS_worker) + t_i;
               auto addr = desc.start + 64 +  (thread_id * 64);
               auto start = utils::getTimePoint();
               // -------------------------------------------------------------------------------------
               rdma::postRead(old, *rctx, rdma::completion::signaled, addr, 8, 0);
               poll_cq(rctx);
               // -------------------------------------------------------------------------------------
               auto end = utils::getTimePoint();
               threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
            running_threads_counter--;
         });
      }
      // -------------------------------------------------------------------------------------
      // Join Threads
      // -------------------------------------------------------------------------------------
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) {
         _mm_pause();
      }
      compute.getWorkerPool().joinAll();
      // -------------------------------------------------------------------------------------
      compute.stopProfiler();
   }
   return 0;
}
