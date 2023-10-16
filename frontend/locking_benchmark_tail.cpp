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
DEFINE_bool(order_release, false, "");
DEFINE_uint64(padding, 8, "");

static constexpr uint64_t UNLOCKED = 0;


static constexpr uint64_t TUPLE_SIZE = 64;  // spans multiple cl to get the correctness.
static constexpr uint64_t LOCK_OFFSET_BYTE = TUPLE_SIZE-8;  // spans multiple cl to get the correctness.
static constexpr uint64_t LOCK_OFFSET_INDEX = LOCK_OFFSET_BYTE / sizeof(uint64_t);  // spans multiple cl to get the correctness.

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
      auto desc = db.getMemoryRegion("block");
      uint8_t* buffer = (uint8_t*)desc.start;
      // -------------------------------------------------------------------------------------
      while (db.getCM().getNumberIncomingConnections()) {
         // debug
         for (uint64_t t_i = 0; t_i < FLAGS_lock_count; t_i++) {
            auto lock_addr = (uint64_t*)(buffer + (t_i * TUPLE_SIZE) + (t_i * FLAGS_padding));
            std::cout << lock_addr[LOCK_OFFSET_INDEX] << "\n";
         }
      }
      // make consistency check
      sleep(5); // drain all open requests

      uint64_t count =0;
      for(uint64_t t_i = 0; t_i < FLAGS_lock_count; t_i++){
         auto lock_addr = (uint64_t*)(buffer + (t_i * TUPLE_SIZE) + (t_i * FLAGS_padding));
         count += lock_addr[1];
         uint64_t prev_version = lock_addr[0];
         for (uint64_t i = 0; i < (TUPLE_SIZE-8) / sizeof(uint64_t); ++i) {
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
      std::string benchmark = "tail-locking-basic";
      if (FLAGS_speculative_read) { benchmark += "+speculative_read"; }
      if (FLAGS_order_release) { benchmark += "+order_release"; }
      // -------------------------------------------------------------------------------------
      std::vector<std::string> workload_type;  // warm up or benchmark
      std::vector<uint32_t> workloads;
      std::vector<double> zipfs;
      // workloads.push_back(5);
      // workloads.push_back(100);
      workloads.push_back(50);
      // workloads.push_back(0);
      // zipfs.insert(zipfs.end(), {0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0});
      zipfs.insert(zipfs.end(), {0});
      // -------------------------------------------------------------------------------------
      u64 lock_count = FLAGS_lock_count;
      // -------------------------------------------------------------------------------------
      std::atomic<uint64_t> g_updates =0;
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
                  auto addr = desc.start;
                  uint64_t* old = reinterpret_cast<uint64_t*>(buffer);

                  auto poll_cq = [&]() {
                     int comp{0};
                     ibv_wc wcReturn;
                     while (comp == 0) {
                        _mm_pause();
                        comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
                     }
                     if (wcReturn.status != IBV_WC_SUCCESS) { throw std::runtime_error("wc failed"); }
                  };

                  // basic lock
                  auto basic_x_lock = [&](uint64_t lock_addr) {
                     volatile uint64_t& x_locked = old[LOCK_OFFSET_INDEX];
                     rdma::postCompareSwap(UNLOCKED, 1, &old[LOCK_OFFSET_INDEX], *(rctx), rdma::completion::signaled, lock_addr+LOCK_OFFSET_BYTE);
                     poll_cq();
                     if (x_locked != UNLOCKED) { return false; }
                     rdma::postRead(old, *rctx, rdma::completion::signaled, lock_addr, TUPLE_SIZE, 0);
                     poll_cq();
                     return true;
                  };
                  // + speculative read
                  auto speculative_read_x_lock = [&](uint64_t lock_addr) {
                     volatile uint64_t& x_locked = old[LOCK_OFFSET_INDEX];
                     rdma::postCompareSwap(UNLOCKED, 1, &old[LOCK_OFFSET_INDEX], *(rctx), rdma::completion::unsignaled, lock_addr+ LOCK_OFFSET_BYTE);
                     // do not read lock value again since could be f&a
                     rdma::postRead(&old[0], *rctx, rdma::completion::signaled, lock_addr, TUPLE_SIZE - 8, 0);
                     poll_cq();
                     if (x_locked != UNLOCKED) { return false; } // must be unlocked
                     return true;
                  };

                  // -------------------------------------------------------------------------------------
                  // READ
                  // -------------------------------------------------------------------------------------
                  auto s_unlock = [&](uint64_t lock_addr) {
                     rdma::postFetchAdd(int64_t{-2}, &old[LOCK_OFFSET_INDEX], *(rctx), rdma::completion::signaled, lock_addr+LOCK_OFFSET_BYTE);
                     poll_cq();
                  };
                  // + basic_read
                  auto basic_s_lock = [&](uint64_t lock_addr) {
                     volatile uint64_t& s_locked = old[LOCK_OFFSET_INDEX];
                     rdma::postFetchAdd(2, &old[LOCK_OFFSET_INDEX], *(rctx), rdma::completion::signaled, lock_addr+LOCK_OFFSET_BYTE);
                     poll_cq();

                     if ((s_locked & 1) == 1) {
                        _mm_pause();
                        return false;
                     }
                     // -------------------------------------------------------------------------------------
                     // read
                     rdma::postRead(old, *rctx, rdma::completion::signaled, lock_addr, TUPLE_SIZE-8, 0);
                     poll_cq();
                     return true;
                  };

                  // + speculative read
                  auto speculative_read_s_lock = [&](uint64_t lock_addr) {
                     volatile uint64_t& s_locked = old[LOCK_OFFSET_INDEX];
                     rdma::postFetchAdd(2, &old[LOCK_OFFSET_INDEX], *(rctx), rdma::completion::unsignaled, lock_addr+LOCK_OFFSET_BYTE);
                     rdma::postRead(&old[0], *rctx, rdma::completion::signaled, lock_addr, TUPLE_SIZE-8, 0);
                     poll_cq();
                     if ((s_locked & 1) == 1) {
                        _mm_pause();
                        return false;
                     }
                     return true;
                  };

                  // + order_release
                  auto s_order_release = [&](uint64_t lock_addr){
                     // with fence! 
                     rdma::postFetchAdd(int64_t{-2}, &old[LOCK_OFFSET_INDEX], *(rctx), rdma::completion::unsignaled, lock_addr+LOCK_OFFSET_INDEX, true);
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
                        if (FLAGS_speculative_read)
                           locked = speculative_read_s_lock(lock_addr);
                        else
                           locked = basic_s_lock(lock_addr);
                        if (!locked) continue;
                        // verify cl counter
                        uint64_t prev_version = old[0];
                        for (uint64_t i = 0; i < (LOCK_OFFSET_BYTE) / sizeof(uint64_t); ++i) {
                           if (prev_version != old[i]) {
                              std::cout << "prev " << prev_version << " " << old[i] << std::endl;
                              throw;
                           }
                           prev_version = old[i];
                        }
                        if(FLAGS_order_release)
                           s_order_release(lock_addr);
                        else
                           s_unlock(lock_addr);
                        auto end = utils::getTimePoint();
                        threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                     } else {
                        auto start = utils::getTimePoint();
                        // write lock
                        bool locked = false;
                        if (FLAGS_speculative_read)
                           locked = speculative_read_x_lock(lock_addr);
                        else
                           locked = basic_x_lock(lock_addr);
                        if (!locked) continue;
                        // increment counter
                        uint64_t new_version = ++old[0];
                        for (uint64_t i = 0; i < (LOCK_OFFSET_BYTE) / sizeof(uint64_t); ++i) {
                           old[i] = new_version;
                        }
                        // write back
                        old[LOCK_OFFSET_INDEX] = 0; // unlock
                        if (FLAGS_order_release){
                           rdma::postWrite(old, *rctx, rdma::completion::unsignaled, lock_addr, TUPLE_SIZE);
                        } else {
                           rdma::postWrite(old, *rctx, rdma::completion::signaled, lock_addr, TUPLE_SIZE);
                           poll_cq();
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
         }
         std::cout << "updates " << g_updates << "\n";
      }
   }
   return 0;
}
