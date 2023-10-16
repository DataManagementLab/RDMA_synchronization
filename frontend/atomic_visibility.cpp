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

int main(int argc, char* argv[]) {
   using namespace nam;
   // -------------------------------------------------------------------------------------
   gflags::SetUsageMessage("Storage-DB Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   if (FLAGS_storage_node) {
      std::cout << "Storage Node" << std::endl;
      nam::Storage db;
      db.registerMemoryRegion("block", 1024 * 1024 * 1024);
      db.startAndConnect();
      // -------------------------------------------------------------------------------------
      // scans the remote writes
      while (db.getCM().getNumberIncomingConnections()) {}
   } else {
      nam::Compute compute;
      uint64_t inconsistencies = 0;
      uint64_t blocks_read = 0;
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      std::string benchmark = "atomic_inconsistency";
      benchmark::SYNC_workloadInfo experimentInfo{benchmark, inconsistencies, blocks_read, 0, 0};
      compute.startProfiler(experimentInfo);

      compute.getWorkerPool().scheduleJobAsync(0, [&]() {
         running_threads_counter++;
         auto& cm = compute.getCM();
         auto* rctx = threads::Worker::my().cctxs[0].rctx;
         auto desc = threads::Worker::my().catalog[0];
         auto lock_addr = desc.start;
         auto* tl_rdma_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
         std::atomic<uint64_t> current_counter = 0;
         while (keep_running) {
            auto start = utils::getTimePoint();
            // add atomic operations
            auto* old = reinterpret_cast<uint64_t*>(tl_rdma_buffer);
            current_counter++;
            old[0]=current_counter;
            volatile uint64_t& counter = old[0];
            
            // -------------------------------------------------------------------------------------
            //write
            // -------------------------------------------------------------------------------------
            rdma::postWrite(old, *rctx, rdma::completion::signaled, lock_addr, 64);
            {
               int comp{0};
               ibv_wc wcReturn;
               while (comp == 0) {
                  comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
                  if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
               }
            }
            // -------------------------------------------------------------------------------------
            // validation read 
            rdma::postRead(old, *rctx, rdma::completion::signaled, lock_addr, 64, 0);
            {
               int comp{0};
               ibv_wc wcReturn;
               while (comp == 0) {
                  comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
                  if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
               }
            }
            if(counter != current_counter){
               throw std::runtime_error("inconsistency detected");
            }
            
            auto end = utils::getTimePoint();
            threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
         running_threads_counter--;
      });

      if (FLAGS_worker > 1) {
         compute.getWorkerPool().scheduleJobAsync(1, [&]() {
            running_threads_counter++;
            auto& cm = compute.getCM();
            auto* rctx = threads::Worker::my().cctxs[0].rctx;
            auto desc = threads::Worker::my().catalog[0];
            auto lock_addr = desc.start;
            auto* tl_rdma_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
            while (keep_running) {
               auto start = utils::getTimePoint();
               // add atomic operations
               auto* old = reinterpret_cast<uint64_t*>(tl_rdma_buffer);
               rdma::postFetchAdd(uint64_t{0}, old, *(rctx), rdma::completion::signaled, lock_addr);
               int comp{0};
               ibv_wc wcReturn;
               while (comp == 0) {
                  comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
                  if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
               }
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
