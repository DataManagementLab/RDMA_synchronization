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

constexpr size_t block_size =512;

DEFINE_double(run_for_seconds, 10.0, "");

int main(int argc, char* argv[]) {
   gflags::SetUsageMessage("Storage-DB Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   using namespace nam;

   if (FLAGS_storage_node) {
      std::cout << "Storage Node" << std::endl;
      nam::Storage db;
      db.registerMemoryRegion("block", 1024 * 1024 * 1024);
      db.startAndConnect();
      // -------------------------------------------------------------------------------------
      // local writer;
      auto desc = db.getMemoryRegion("block");
      auto number_entries = desc.size_bytes / sizeof(uint64_t);
      uint64_t version =1;
      uint64_t* buffer = (uint64_t*)desc.start;
      while(db.getCM().getNumberIncomingConnections()){
         version++;
         std::cout << "run " << version << "\n";
         for (size_t e_i = 0; e_i < number_entries; ++e_i)
         {
            buffer[e_i] = version;
         }
      }

   } else {
      std::cout << "Compute Node" << std::endl;
      nam::Compute compute;
      uint64_t inconsistencies = 0;
      uint64_t blocks_read = 0;
      benchmark::SYNC_workloadInfo experimentInfo{"broken_read_ordering", inconsistencies, blocks_read, block_size, 0};
      compute.startProfiler(experimentInfo);
      // -------------------------------------------------------------------------------------
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      // -------------------------------------------------------------------------------------
      // Reader
      compute.getWorkerPool().scheduleJobAsync(0, [&]() {
         auto& cm = compute.getCM();
         auto* rctx = threads::Worker::my().cctxs[0].rctx;
         auto desc = threads::Worker::my().catalog[0];
         auto number_blocks = desc.size_bytes / block_size;
         std::cout << "Number of blocks " << number_blocks << "\n";
         auto* buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(block_size, block_size));
         running_threads_counter++;
         while (keep_running) {
            benchmark::readBlocks(desc.start, buffer, block_size, number_blocks, blocks_read, inconsistencies, *rctx, keep_running,
                                  [&]( [[maybe_unused]] uint64_t number_entries, uint64_t remote_addr, uint64_t*& buffer, nam::rdma::RdmaContext& rctx) {
                                     rdma::postRead(buffer, rctx, rdma::completion::signaled, remote_addr, block_size, 0);
                                     // -------------------------------------------------------------------------------------
                                     int comp{0};
                                     ibv_wc wcReturn;
                                     while (comp == 0) {
                                        comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
                                     }
                                  });
         }
         running_threads_counter--;
      });
      // -------------------------------------------------------------------------------------
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
