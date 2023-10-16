#include "BenchmarkHelper.hpp"
#include "Defs.hpp"
#include "OptimisticLocks.hpp"
#include "PerfEvent.hpp"
#include "nam/Compute.hpp"
#include "nam/Config.hpp"
#include "nam/Storage.hpp"
#include "exception_hack.hpp"
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
DEFINE_uint64(sleep, 0, "sleep in microseconds ");

static constexpr uint64_t TUPLE_SIZE = 4096;  // spans multiple cl to get the correctness.
static constexpr uint64_t B = 249;            // fanout for 16 byte - lock
static constexpr uint64_t H = 3;              // height of 3

int main(int argc, char* argv[]) {
   exception_hack::init_phdr_cache();
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
      // make consistency check
   } else {
      nam::Compute compute;
      std::string benchmark = "Opt. B-Tree traversal";
      // -------------------------------------------------------------------------------------
      std::vector<std::string> workload_type;  // warm up or benchmark
      // -------------------------------------------------------------------------------------
      std::atomic<uint64_t> g_updates = 0;
      uint64_t stage = 1;
      // -------------------------------------------------------------------------------------
      uint64_t nodes = 0;
      for (uint64_t h_i = 0; h_i < H; h_i++) {
         nodes += std::pow(B, h_i);
      }
      // -------------------------------------------------------------------------------------
      crc64_init();
      // -------------------------------------------------------------------------------------

      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      benchmark::LOCKING_BENCHMARK_workloadInfo experimentInfo{benchmark, nodes, 100, 0, std::to_string(FLAGS_padding)};
      compute.startProfiler(experimentInfo);
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         compute.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            uint64_t updates = 0;
            auto& cm = compute.getCM();
            auto* rctx = threads::Worker::my().cctxs[0].rctx;
            auto desc = threads::Worker::my().catalog[0];
            auto* buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(TUPLE_SIZE * 2, 64));
            uint64_t* barrier_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
            auto addr = desc.start + 64;
            uint64_t* tuple_buffer = reinterpret_cast<uint64_t*>(buffer);

            auto barrier_addr = desc.start;
            rdma_barrier_wait(barrier_addr, stage, barrier_buffer, *rctx);

            running_threads_counter++;
            while (keep_running) {
               uint64_t next_idx = 0;  // root
               uint64_t prev_id = 0;   // previous b-tree node_id
               for (uint64_t h_i = 0; h_i < H; h_i++) {
                  // node address
                  auto lock_addr = addr + (next_idx * TUPLE_SIZE) + (next_idx * FLAGS_padding);
                  auto start = utils::getTimePoint();
                  // read lock
                  OptimisticLock<FaRM, FooterLock> tuple(*rctx, lock_addr, tuple_buffer, TUPLE_SIZE);
                  for (uint64_t repeatCounter = 0;; repeatCounter++) {
                     try {
                        tuple.lock();
                        // go over the entries
                        [[maybe_unused]] auto versions = tuple.unlock();
                        // after the validation we have a consistent snapshot
                        uint64_t prev = tuple_buffer[3];
                        bool corrupt = false;
                        for (uint64_t cl_i = 0; cl_i < (TUPLE_SIZE / CL); cl_i++) {
                           for (uint64_t v_i = 3; v_i < 6; v_i++) {
                              auto idx = (cl_i * 8) + v_i;
                              if (prev != tuple_buffer[idx]) { corrupt = true; }
                           }
                        }
                        if (corrupt) {
                           for (uint64_t cl_i = 0; cl_i < (TUPLE_SIZE / CL); cl_i++) {
                              for (uint64_t v_i = 0; v_i < 8; v_i++) {
                                 auto idx = (cl_i * 8) + v_i;
                                 std::cout << tuple_buffer[idx] << "  ";
                              }
                           }
                           std::cout << " " << std::endl;
                           std::cout << "versions " << versions.first << " " << versions.second << std::endl;
                           throw std::runtime_error("Read consistency check failed ");
                        }
                        // -------------------------------------------------------------------------------------
                        break;
                     } catch (const OLRestartException&) { threads::Worker::my().counters.incr(profiling::WorkerCounters::abort); }
                  }

                  auto end = utils::getTimePoint();
                  threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                  // calculate next node
                  auto node_id = utils::RandomGenerator::getRandU64(0, B);
                  next_idx = (B * h_i + 1) + (B * prev_id) + node_id;
                  prev_id = node_id;
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
   return 0;
}
