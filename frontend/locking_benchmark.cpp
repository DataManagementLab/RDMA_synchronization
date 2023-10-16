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
DEFINE_bool(speculative_read, false, "");
DEFINE_bool(write_combining, false, "");
DEFINE_bool(order_release, false, "");
DEFINE_uint64(padding, 8, "");
DEFINE_uint64(sleep, 0, "sleep in microseconds ");

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
      // make consistency check
      sleep(5); // drain all open requests
      auto desc = db.getMemoryRegion("block");
      uint8_t* buffer = (uint8_t*)desc.start +64;
      uint64_t count =0;
      for(uint64_t t_i = 0; t_i < FLAGS_lock_count; t_i++){
         auto lock_addr = (uint64_t*)(buffer + (t_i * TUPLE_SIZE) + (t_i * FLAGS_padding));
         count += lock_addr[1];
         uint64_t prev_version = lock_addr[1];
         for (uint64_t i = 1; i < TUPLE_SIZE / sizeof(uint64_t); ++i) {
            if (prev_version != lock_addr[i]) {
               std::cout << "prev " << prev_version << " " << lock_addr[i] << std::endl;
               throw;
            }
            prev_version = lock_addr[i];
         }
      }
      std::cout << "aggregated updates  " << count << "\n";

      
   } else {
      nam::Compute compute;
      std::string benchmark = "locking-basic";
      if (FLAGS_speculative_read) { benchmark += "+speculative_read"; }
      if (FLAGS_write_combining) { benchmark += "+write_combining"; }
      if (FLAGS_order_release) { benchmark += "+order_release_wo_fence"; }
      if (FLAGS_sleep > 0) { benchmark += "sleep_inbetween" + std::to_string(FLAGS_sleep); }
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
                  auto* rctx = threads::Worker::my().cctxs[0].rctx;
                  auto desc = threads::Worker::my().catalog[0];
                  auto* buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(1024, 64));
                  uint64_t* barrier_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
                  auto addr = desc.start + 64;
                  uint64_t* old = reinterpret_cast<uint64_t*>(buffer);

                  auto barrier_addr = desc.start;
                  rdma_barrier_wait(barrier_addr,stage,barrier_buffer, *rctx );
                  
                  auto poll_cq = [&]() {
                     int comp{0};
                     ibv_wc wcReturn;
                     while (comp == 0) {
                        _mm_pause();
                        comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
                     }
                  };
                  // -------------------------------------------------------------------------------------
                  // WRITE
                  // -------------------------------------------------------------------------------------
                  auto x_unlock = [&](uint64_t lock_addr) {
                     rdma::postFetchAdd(EXCLUSIVE_UNLOCK_TO_BE_ADDED, old, *(rctx), rdma::completion::signaled, lock_addr);
                     poll_cq();
                  };
                  // basic lock
                  auto basic_x_lock = [&](uint64_t lock_addr) {
                     volatile uint64_t& x_locked = old[0];
                     rdma::postCompareSwap(UNLOCKED, EXCLUSIVE_LOCKED, old, *(rctx), rdma::completion::signaled, lock_addr);
                     poll_cq();
                     if (x_locked != UNLOCKED) { return false; }
                     rdma::postRead(old, *rctx, rdma::completion::signaled, lock_addr, TUPLE_SIZE, 0);
                     poll_cq();
                     return true;
                  };
                  // + speculative read
                  auto speculative_read_x_lock = [&](uint64_t lock_addr) {
                     volatile uint64_t& x_locked = old[0];
                     rdma::postCompareSwap(UNLOCKED, EXCLUSIVE_LOCKED, old, *(rctx), rdma::completion::unsignaled, lock_addr);
                     // do not read lock value again since could be f&a
                     rdma::postRead(&old[1], *rctx, rdma::completion::signaled, lock_addr + 8, TUPLE_SIZE - 8, 0);
                     poll_cq();
                     if (x_locked != UNLOCKED) { return false; } // must be unlocked
                     return true;
                  };
                  // + write combining
                  auto write_combining = [&](uint64_t lock_addr) {
                     rdma::postWrite(&old[1], *rctx, rdma::completion::unsignaled, lock_addr + 8, TUPLE_SIZE - 8);
                     x_unlock(lock_addr);
                  };
                  // + order_release
                  auto x_order_release = [&](uint64_t lock_addr) {
                     rdma::postWrite(&old[1], *rctx, rdma::completion::unsignaled, lock_addr + 8, TUPLE_SIZE - 8);
                     // with fence! should ensure that memory buffer is not overwritten
                     rdma::postFetchAdd(EXCLUSIVE_UNLOCK_TO_BE_ADDED, old, *(rctx), rdma::completion::unsignaled, lock_addr, false);
                     _mm_pause();
                  };
                  // -------------------------------------------------------------------------------------
                  // READ
                  // -------------------------------------------------------------------------------------
                  auto s_unlock = [&](uint64_t lock_addr) {
                     rdma::postFetchAdd(int64_t{-1}, old, *(rctx), rdma::completion::signaled, lock_addr);
                     poll_cq();
                  };
                  // + basic_read
                  auto basic_s_lock = [&](uint64_t lock_addr) {
                     volatile uint64_t& s_locked = old[0];
                     rdma::postFetchAdd(1, old, *(rctx), rdma::completion::signaled, lock_addr);
                     poll_cq();

                     if (s_locked >= EXCLUSIVE_LOCKED) {
                        s_unlock(lock_addr);
                        return false;
                     }
                     // -------------------------------------------------------------------------------------
                     // read
                     rdma::postRead(old, *rctx, rdma::completion::signaled, lock_addr, TUPLE_SIZE, 0);
                     poll_cq();
                     return true;
                  };

                  // + speculative read
                  auto speculative_read_s_lock = [&](uint64_t lock_addr) {
                     volatile uint64_t& s_locked = old[0];
                     rdma::postFetchAdd(1, old, *(rctx), rdma::completion::unsignaled, lock_addr);
                     // -------------------------------------------------------------------------------------
                     if (FLAGS_sleep > 0) {
                        for (size_t i = 0; i < FLAGS_sleep; ++i) {
                           _mm_pause();
                        }
                     }
                     // -------------------------------------------------------------------------------------
                     rdma::postRead(&old[1], *rctx, rdma::completion::signaled, lock_addr + 8, TUPLE_SIZE - 8, 0);
                     poll_cq();
                     if (s_locked >= EXCLUSIVE_LOCKED) {
                        s_unlock(lock_addr);
                        return false;
                     }
                     return true;
                  };

                  // + order_release
                  auto s_order_release = [&](uint64_t lock_addr){
                     // with fence! 
                     rdma::postFetchAdd(int64_t{-1}, old, *(rctx), rdma::completion::unsignaled, lock_addr, false);
                     _mm_pause();
                  };

                  running_threads_counter++;
                  while (keep_running) {
                     uint64_t lock_id = zipf_random->rand(0);
                     auto lock_addr = addr + (lock_id * TUPLE_SIZE) + (lock_id * FLAGS_padding);

                     ensure(lock_id < lock_count);
                     if (READ_RATIO == 100 || utils::RandomGenerator::getRandU64(0, 100) < READ_RATIO) {
                        auto start = utils::getTimePoint();
                        // read lock
                        bool locked = false;
                        if (FLAGS_speculative_read) {
                           locked = speculative_read_s_lock(lock_addr);
                        } else
                           locked = basic_s_lock(lock_addr);
                        if (!locked) continue;
                        // verify cl counter
                        uint64_t prev_version = old[1];
                        for (uint64_t i = 1; i < TUPLE_SIZE / sizeof(uint64_t); ++i) {
                           if (prev_version != old[i]) {
                              std::cout << "prev " << prev_version << " " << old[i] << std::endl;
                              throw;
                           }
                           prev_version = old[i];
                        }
                        if (FLAGS_order_release)
                           s_order_release(lock_addr);
                        else
                           s_unlock(lock_addr);
                        auto end = utils::getTimePoint();
                        threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                     } else {
                        auto start = utils::getTimePoint();
                        // write lock
                        bool locked = false;
                        if (FLAGS_speculative_read) {
                           locked = speculative_read_x_lock(lock_addr);
                           if (FLAGS_sleep > 0) {
                              for (size_t i = 0; i < FLAGS_sleep; ++i) {
                                 _mm_pause();
                              }
                           }
                        } else
                           locked = basic_x_lock(lock_addr);
                        if (!locked) continue;
                        // increment counter
                        uint64_t new_version = ++old[1];
                        for (uint64_t i = 1; i < TUPLE_SIZE / sizeof(uint64_t); ++i) {
                           old[i] = new_version;
                        }
                        // write back
                        if (FLAGS_write_combining) {
                           if (FLAGS_order_release)
                              x_order_release(lock_addr);
                           else
                              write_combining(lock_addr);
                        } else {
                           rdma::postWrite(&old[1], *rctx, rdma::completion::signaled, lock_addr + 8, TUPLE_SIZE - 8);
                           poll_cq();
                           x_unlock(lock_addr);
                        }
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
         std::cout << "updates " << g_updates << "\n";
      }
   }
   return 0;
}
