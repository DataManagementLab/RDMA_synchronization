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


int main(int argc, char* argv[]) {
   gflags::SetUsageMessage("Storage-DB Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   using namespace nam;

   if (FLAGS_storage_node) {
      std::cout << "Storage Node" << std::endl;
      nam::Storage db;
      db.registerMemoryRegion("block", 1024 * 1024 * 50);
      db.startAndConnect();
      // -------------------------------------------------------------------------------------
      // scans the remote writes 
      while(db.getCM().getNumberIncomingConnections()){
      }

   } else {
      std::cout << "Compute Node" << std::endl;
      nam::Compute compute;
      uint64_t inconsistencies = 0;
      uint64_t blocks_read = 0;
      benchmark::SYNC_workloadInfo experimentInfo{"broken_read_ordering_rwrite", inconsistencies, blocks_read, FLAGS_block_size,FLAGS_write_speed};
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
         auto number_blocks = desc.size_bytes / FLAGS_block_size;
         std::cout << "Number of blocks " << number_blocks << "\n";
         auto* buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(FLAGS_block_size, FLAGS_block_size));
         uint64_t version =1;
         running_threads_counter++;
         auto number_entries = FLAGS_block_size / sizeof(uint64_t); // per block
         uint64_t wqe = 0;
         uint64_t SIGNAL_ = FLAGS_write_speed -1;
         while (keep_running) {
            version++;
            // fill buffer with current version and sent writes
            for (size_t e_i = 0; e_i < number_entries; ++e_i) {
               buffer[e_i] = version;
            }
            for (size_t b_i = 0; b_i < number_blocks; ++b_i) {
               if(!keep_running) break;
               size_t remote_addr = desc.start + (FLAGS_block_size * b_i);
               rdma::completion  signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
               rdma::postWrite(buffer, *rctx, signal, remote_addr, FLAGS_block_size);
               wqe++;
               // -------------------------------------------------------------------------------------
               if ((wqe & SIGNAL_) == SIGNAL_) {
                  int comp{0};
                  ibv_wc wcReturn;
                  while (comp == 0) {
                     comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
                  }
               }
            }
         }
         running_threads_counter--;
      });
      // -------------------------------------------------------------------------------------
      // Reader
      compute.getWorkerPool().scheduleJobAsync(1, [&]() {
         auto& cm = compute.getCM();
         auto* rctx = threads::Worker::my().cctxs[0].rctx;
         auto desc = threads::Worker::my().catalog[0];
         auto number_blocks = desc.size_bytes / FLAGS_block_size;
         std::cout << "Number of blocks " << number_blocks << "\n";
         auto* buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(FLAGS_block_size, FLAGS_block_size));
         running_threads_counter++;
         while (keep_running) {
            benchmark::readBlocks(
                desc.start, buffer, FLAGS_block_size, number_blocks, blocks_read, inconsistencies, *rctx, keep_running,
                [&]([[maybe_unused]] uint64_t number_entries, uint64_t remote_addr, uint64_t*& buffer, nam::rdma::RdmaContext& rctx) {
                   rdma::postRead(buffer, rctx, rdma::completion::signaled, remote_addr, FLAGS_block_size, 0);
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
