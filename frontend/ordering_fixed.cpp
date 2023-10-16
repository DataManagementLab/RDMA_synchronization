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
      uint64_t version = 1;
      uint64_t* buffer = (uint64_t*)desc.start;
      while (true) {
         version++;
         for (size_t e_i = 0; e_i < number_entries; ++e_i) {
            buffer[e_i] = version;
         }
      }

   } else {
      std::cout << "Compute Node" << std::endl;
      nam::Compute compute;
      uint64_t inconsistencies = 0;
      uint64_t blocks_read = 0;
      std::atomic<bool> keep_running = true;
      benchmark::SYNC_workloadInfo experimentInfo{"fixed_ordering_64b", inconsistencies, blocks_read, block_size,0};
      compute.startProfiler(experimentInfo);
      // -------------------------------------------------------------------------------------
      // Reader
      compute.getWorkerPool().scheduleJobAsync(0, [&]() {
         auto& cm = compute.getCM();
         auto* rctx = threads::Worker::my().cctxs[0].rctx;
         auto desc = threads::Worker::my().catalog[0];
         auto number_blocks = desc.size_bytes / block_size;
         std::cout << "Number of blocks " << number_blocks << "\n";
         auto* buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(block_size, block_size));
         // auto number_entries = block_size / sizeof(uint64_t);
         while (true) {
            benchmark::readBlocks(desc.start, buffer, block_size, number_blocks, blocks_read, inconsistencies, *rctx, keep_running,
                                  [&](uint64_t number_entries, uint64_t remote_addr, uint64_t*& buffer, nam::rdma::RdmaContext& rctx) {
                                     for (size_t e_i = 0; e_i < number_entries; ++e_i) {
                                        auto comp = (e_i == number_entries - 1) ? rdma::completion::signaled : rdma::completion::unsignaled;
                                        rdma::postRead(buffer, rctx, comp, remote_addr + (e_i * 64), 64, 0);
                                     }
                                     // -------------------------------------------------------------------------------------
                                     int comp{0};
                                     ibv_wc wcReturn;
                                     while (comp == 0) {
                                        comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
                                     }
                                  });
         }
      });
      compute.getWorkerPool().joinAll();
      compute.stopProfiler();
   }
   return 0;
}
