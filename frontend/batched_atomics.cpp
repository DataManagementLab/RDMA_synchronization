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
DEFINE_uint64(padding, 8, "");
DEFINE_uint64(batch, 8, "");

static constexpr uint64_t EXCLUSIVE_LOCKED = 0x1000000000000000;
static constexpr uint64_t EXCLUSIVE_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF - EXCLUSIVE_LOCKED + 1;
static constexpr uint64_t UNLOCKED = 0;
static constexpr uint64_t MASKED_SHARED_LOCKS = 0x1000000000000000;
static constexpr uint64_t SHARED_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF;

static constexpr uint64_t TUPLE_SIZE = 256;  // spans multiple cl to get the correctness.

int main(int argc, char* argv[]) {
   gflags::SetUsageMessage("Storage-DB Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   using namespace nam;

   if (FLAGS_storage_node) {
      ensure(((FLAGS_lock_count * TUPLE_SIZE) + (FLAGS_lock_count * FLAGS_padding)) < (FLAGS_dramGB * 1024 * 1024 * 1024));
      std::cout << "Storage Node" << std::endl;
      nam::Storage db;
      db.registerMemoryRegion("block", FLAGS_dramGB * 1024 * 1024 * 1024);
      db.startAndConnect();
      // -------------------------------------------------------------------------------------
      while (db.getCM().getNumberIncomingConnections()) {}
   } else {
      nam::Compute compute;
      std::string benchmark = "FA_atomic";
      // -------------------------------------------------------------------------------------
      std::vector<std::string> workload_type;  // warm up or benchmark
      std::vector<double> zipfs;
      // zipfs.insert(zipfs.end(), {0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0});
      zipfs.insert(zipfs.end(), {0});
      // -------------------------------------------------------------------------------------
      u64 lock_count = FLAGS_lock_count;
      // -------------------------------------------------------------------------------------
      std::atomic<uint64_t> g_updates = 0;
      for (auto ZIPF : zipfs) {
         std::unique_ptr<utils::ScrambledZipfGenerator> zipf_random;
         zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, lock_count, ZIPF);
         // -------------------------------------------------------------------------------------
         std::atomic<bool> keep_running = true;
         std::atomic<u64> running_threads_counter = 0;
         benchmark::BATCHING_BENCHMARK_workloadInfo experimentInfo{benchmark, FLAGS_lock_count, FLAGS_batch, std::to_string(FLAGS_padding)};
         compute.startProfiler(experimentInfo);
         for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
            compute.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
               uint64_t updates = 0;
               auto& cm = compute.getCM();
               std::vector<uint64_t*> buffers(FLAGS_batch);
               uint64_t* barrier_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
               for (uint64_t b_i = 0; b_i < FLAGS_batch; b_i++) {
                  buffers[b_i] = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(TUPLE_SIZE, 64));
               }
               std::atomic<uint64_t> inconsistencies = 0;
               auto poll_cq = [&](rdma::RdmaContext*& rctx) {
                  int comp{0};
                  ibv_wc wcReturn;
                  while (comp == 0) {
                     _mm_pause();
                     comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
                  }
                  if (wcReturn.status != IBV_WC_SUCCESS) { throw std::runtime_error("wc failed"); }
               };
               // -------------------------------------------------------------------------------------
               auto& catalog = threads::Worker::my().catalog;
               running_threads_counter++;
               auto& cctxs = threads::Worker::my().cctxs;
               uint64_t current_node = 0;
               // wait on barrier which is always node 0 and in the first cl
               auto barrier_addr = catalog[0].start;
               rdma_barrier_wait(barrier_addr,1,barrier_buffer, *cctxs[0].rctx );
               
               while (keep_running) {
                  uint64_t s_id = current_node % FLAGS_storage_nodes;
                  current_node++;
                  auto* rctx = cctxs[s_id].rctx;
                  auto desc = catalog[s_id];

                  auto addr = desc.start+64;


                  auto start = utils::getTimePoint();
                  // read
                  for (uint64_t b_i = 0; b_i < FLAGS_batch; b_i++) {
                     uint64_t lock_id = utils::RandomGenerator::getRandU64Fast() % lock_count;
                     ensure(lock_id < lock_count);
                     auto lock_addr = addr + (lock_id * TUPLE_SIZE) + (lock_id * FLAGS_padding);
                     auto comp = (b_i == FLAGS_batch - 1) ? rdma::completion::signaled : rdma::completion::unsignaled;
                     rdma::postFetchAdd(1, buffers[b_i], *(rctx), comp, lock_addr);
                  }
                  poll_cq(rctx);
                  auto end = utils::getTimePoint();
                  threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                  threads::Worker::my().counters.incr_by(profiling::WorkerCounters::tx_p, FLAGS_batch);
               }
               g_updates += updates;
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
   }
   return 0;
}
