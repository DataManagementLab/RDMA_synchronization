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
      std::string benchmark = "no_locking";
      // -------------------------------------------------------------------------------------
      std::vector<std::string> workload_type;  // warm up or benchmark
      std::vector<uint32_t> workloads;
      std::vector<double> zipfs;
      // workloads.push_back(5);
      workloads.push_back(100);
      workloads.push_back(50);
      workloads.push_back(0);
      // zipfs.insert(zipfs.end(), {0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0});
      zipfs.insert(zipfs.end(), {0});
      // -------------------------------------------------------------------------------------
      u64 lock_count = FLAGS_lock_count;
      // -------------------------------------------------------------------------------------
      std::atomic<uint64_t> g_updates =0;
      uint64_t stage =1;
      for (auto ZIPF : zipfs) {
         std::unique_ptr<utils::ScrambledZipfGenerator> zipf_random;
         zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, lock_count, ZIPF);
         // -------------------------------------------------------------------------------------
         for (auto READ_RATIO : workloads) {
            std::atomic<bool> keep_running = true;
            std::atomic<u64> running_threads_counter = 0;
            benchmark::LOCKING_BENCHMARK_workloadInfo experimentInfo{benchmark, FLAGS_lock_count, READ_RATIO, ZIPF,
                                                                     std::to_string(FLAGS_padding)};
            compute.startProfiler(experimentInfo);
            for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
               compute.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
                  uint64_t updates = 0;
                  auto& cm = compute.getCM();
                  auto* buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(1024, 64));
                  uint64_t* barrier_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
                  uint64_t* old = reinterpret_cast<uint64_t*>(buffer);
                  std::atomic<uint64_t> inconsistencies =0;
                  auto poll_cq = [&](rdma::RdmaContext*& rctx) {
                     int comp{0};
                     ibv_wc wcReturn;
                     while (comp == 0) {
                        _mm_pause();
                        comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
                     }
                     if (wcReturn.status != IBV_WC_SUCCESS) { throw std::runtime_error("wc failed"); }
                  };

                  auto& catalog = threads::Worker::my().catalog;
                  running_threads_counter++;
                  auto& cctxs = threads::Worker::my().cctxs;
                  uint64_t current_node =0;

                  auto barrier_addr = catalog[0].start;
                  rdma_barrier_wait(barrier_addr,stage,barrier_buffer, *cctxs[0].rctx );
                  
                  while (keep_running) {
                     uint64_t s_id = current_node % FLAGS_storage_nodes;
                     current_node++;
                     auto* rctx = cctxs[s_id].rctx;
                     auto desc = catalog[s_id];
                     
                     auto addr = desc.start + 64;
                     
                     uint64_t lock_id = zipf_random->rand(0);
                     auto lock_addr = addr + (lock_id * TUPLE_SIZE) + (lock_id * FLAGS_padding);
                     

                     ensure(lock_id < lock_count);
                     if (READ_RATIO == 100 || utils::RandomGenerator::getRandU64(0, 100) < READ_RATIO) {
                        auto start = utils::getTimePoint();
                        // read
                        rdma::postRead(&old[1], *rctx, rdma::completion::signaled, lock_addr+8, TUPLE_SIZE-8, 0);
                        poll_cq(rctx);
                        uint64_t prev_version = old[1];
                        for (uint64_t i = 1; i < TUPLE_SIZE / sizeof(uint64_t); ++i) {
                           if (prev_version != old[i]) {
                              inconsistencies++;
                           }
                           prev_version = old[i];
                        }
                        auto end = utils::getTimePoint();
                        threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                     } else {
                        auto start = utils::getTimePoint();
                        rdma::postRead(&old[1], *rctx, rdma::completion::signaled, lock_addr+8, TUPLE_SIZE-8, 0);
                        poll_cq(rctx);
                        // write lock
                        uint64_t new_version = ++old[1];
                        for (uint64_t i = 1; i < TUPLE_SIZE / sizeof(uint64_t); ++i) {
                           old[i] = new_version;
                        }
                        rdma::postWrite(&old[1], *rctx, rdma::completion::signaled, lock_addr + 8, TUPLE_SIZE - 8);
                        poll_cq(rctx);
                        auto end = utils::getTimePoint();
                        updates++;
                        threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                     }
                     threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
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
            stage++;
         }

      }
   }
   return 0;
}
