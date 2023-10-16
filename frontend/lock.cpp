#include "Defs.hpp"
#include "PerfEvent.hpp"
#include "nam/Config.hpp"
#include "nam/Storage.hpp"
#include "nam/Compute.hpp"
#include "nam/profiling/ProfilingThread.hpp"
#include "nam/profiling/counters/WorkerCounters.hpp"
#include "nam/threads/Concurrency.hpp"
#include "nam/utils/RandomGenerator.hpp"
#include "nam/utils/Time.hpp"
#include "BenchmarkHelper.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
// -------------------------------------------------------------------------------------


DEFINE_double(run_for_seconds, 10.0, "");
DEFINE_uint64(block_size, 512 ,"");
DEFINE_uint64(write_speed, 16 ,"");
DEFINE_bool(speculative_read, false, "");
DEFINE_bool(write_combining, false, "");
DEFINE_bool(write, true, "");
DEFINE_bool(order_release, false, "");
DEFINE_bool(unlock_write, false, "");


static constexpr uint64_t EXCLUSIVE_LOCKED = 0x1000000000000000;
static constexpr uint64_t EXCLUSIVE_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF - EXCLUSIVE_LOCKED + 1;
static constexpr uint64_t UNLOCKED = 0;
static constexpr uint64_t MASKED_SHARED_LOCKS = 0x1000000000000000;
static constexpr uint64_t SHARED_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF;


int main(int argc, char* argv[]) {
   gflags::SetUsageMessage("Storage-DB Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   using namespace nam;

   if (FLAGS_storage_node) {
      std::cout << "Storage Node" << std::endl;
      nam::Storage db;
      db.registerMemoryRegion("block", 1024 * 1024 * 50);
      // scans the remote writes
      auto desc = db.getMemoryRegion("block");
      uint64_t* buffer = (uint64_t*)desc.start;
      buffer[0] = 0;
      buffer[1] = 10;
      
      
      db.startAndConnect();
      // -------------------------------------------------------------------------------------
      while(db.getCM().getNumberIncomingConnections()){
      }

   } else {
      std::cout << "Compute Node" << std::endl;
      nam::Compute compute;
      std::string benchmark = "locking-basic";
      if (FLAGS_speculative_read) { benchmark += "+speculative_read"; }
      if (FLAGS_write_combining) { benchmark += "+write_combining"; }
      if (FLAGS_order_release) { benchmark += "+order_release"; }
      if (FLAGS_unlock_write) { benchmark += "+unlock_write"; }
      benchmark::LOCKING_workloadInfo experimentInfo{benchmark, FLAGS_write ? "write" :"read"};
      compute.startProfiler(experimentInfo);
      // -------------------------------------------------------------------------------------
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      // -------------------------------------------------------------------------------------
      // Writer
      compute.getWorkerPool().scheduleJobAsync(0, [&]() {
         auto& cm = compute.getCM();
         auto* rctx = threads::Worker::my().cctxs[0].rctx;
         auto desc = threads::Worker::my().catalog[0];
         auto* buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(1024, 64));
         auto addr = desc.start;
         running_threads_counter++;
         auto pollCQ = [&]() {
            {
               int comp{0};
               ibv_wc wcReturn;
               while (comp == 0) {
                  comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
               }
            }
         };

         while (keep_running) {
            // -------------------------------------------------------------------------------------
            // CAS for locking
            // -------------------------------------------------------------------------------------
            auto* old = reinterpret_cast<uint64_t*>(buffer);
            // auto* value = reinterpret_cast<uint64_t*>(&buffer[8]);
            if (FLAGS_write) {
               if (FLAGS_unlock_write) {
                  rdma::postCompareSwap(UNLOCKED, EXCLUSIVE_LOCKED, old, *(rctx), rdma::completion::unsignaled, addr + 56);
                  rdma::postRead(old, *rctx, rdma::completion::signaled, addr, 64, 0);
                  pollCQ();
                  if (old[7] != EXCLUSIVE_LOCKED) {
                     std::cout << "value " << old[7] << "\n";
                     std::cout << "not locked"
                               << "\n";
                     throw;
                  }
                  ensure(old[1] == 10);
                  old[7] = 0;  // unlock
                  rdma::postWrite(old, *rctx, rdma::completion::unsignaled, addr, 64);
               } else {
                  if (FLAGS_speculative_read) {
                     // according to spec that should be fine ... but broken?!
                     rdma::postCompareSwap(UNLOCKED, EXCLUSIVE_LOCKED, old, *(rctx), rdma::completion::unsignaled, addr);
                     rdma::postRead(old, *rctx, rdma::completion::signaled, addr, 64, 0);
                     pollCQ();
                     if (*old != EXCLUSIVE_LOCKED) {
                        std::cout << "value " << *old << "\n";
                        std::cout << "not locked"
                                  << "\n";
                        throw;
                     }
                     ensure(old[1] == 10);
                  } else {
                     rdma::postCompareSwap(UNLOCKED, EXCLUSIVE_LOCKED, old, *(rctx), rdma::completion::signaled, addr);
                     pollCQ();
                     // not locked
                     if (*old != UNLOCKED) {
                        std::cout << "not locked"
                                  << "\n";
                        throw;
                     }
                     // -------------------------------------------------------------------------------------
                     // read
                     rdma::postRead(old, *rctx, rdma::completion::signaled, addr, 64, 0);
                     pollCQ();
                     ensure(old[1] == 10);
                  }
                  // -------------------------------------------------------------------------------------
                  // write
                  if (FLAGS_write_combining) {
                     if (FLAGS_order_release) {
                        rdma::postWrite(old, *rctx, rdma::completion::unsignaled, addr, 64);
                        rdma::postFetchAdd(EXCLUSIVE_UNLOCK_TO_BE_ADDED, old, *(rctx), rdma::completion::unsignaled, addr, true);
                     } else {
                        rdma::postWrite(old, *rctx, rdma::completion::unsignaled, addr, 64);
                        rdma::postFetchAdd(EXCLUSIVE_UNLOCK_TO_BE_ADDED, old, *(rctx), rdma::completion::signaled, addr);
                        pollCQ();
                     }
                  } else {
                     rdma::postWrite(old, *rctx, rdma::completion::signaled, addr, 64);
                     pollCQ();
                     // unlock
                     rdma::postFetchAdd(EXCLUSIVE_UNLOCK_TO_BE_ADDED, old, *(rctx), rdma::completion::signaled, addr);
                     pollCQ();
                  }
               }
            } else {
               // READ
               if (FLAGS_speculative_read) {
                  // according to spec that should be fine ... but broken?!
                  rdma::postFetchAdd(1, old, *(rctx), rdma::completion::unsignaled, addr);
                  rdma::postRead(old, *rctx, rdma::completion::signaled, addr, 64, 0);
                  pollCQ();
                  if (*old != 1) { // st it is fine 
                     std::cout << "value " << *old << "\n";
                     std::cout << "not locked"
                               << "\n";
                     throw;
                  }
                  ensure(old[1] == 10);
               } else {
                  rdma::postFetchAdd(1, old, *(rctx), rdma::completion::signaled, addr);
                  pollCQ();
                  // not locked
                  if (*old != UNLOCKED) {
                     std::cout << "not locked"
                               << "\n";
                     throw;
                  }
                  // -------------------------------------------------------------------------------------
                  // read
                  rdma::postRead(old, *rctx, rdma::completion::signaled, addr, 64, 0);
                  pollCQ();
                  ensure(old[1] == 10);
               }
               // -------------------------------------------------------------------------------------
               // unlatch read
               if (FLAGS_order_release) {
                  // TODO must use fence
                  rdma::postFetchAdd(int64_t{-1}, old, *(rctx), rdma::completion::unsignaled, addr, true);
               } else {
                  rdma::postFetchAdd(int64_t{-1}, old, *(rctx), rdma::completion::signaled, addr);
                  pollCQ();
               }
            }
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
         running_threads_counter--;
      });
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) {
         _mm_pause();
      }
      // -------------------------------------------------------------------------------------
      compute.getWorkerPool().joinAll();
      compute.stopProfiler();
   }
   return 0;
}
