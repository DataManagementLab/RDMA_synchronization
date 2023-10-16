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
DEFINE_uint64(write_size, 64 ,"");

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
      while (db.getCM().getNumberIncomingConnections()) {}

   } else {
      std::cout << "Compute Node" << std::endl;
      nam::Compute compute;
      uint64_t writes = 0;
      uint64_t blocks_read = 0;
      benchmark::SYNC_workloadInfo experimentInfo{"broken_read_ordering_rwrite", writes, blocks_read, 0, 0};
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
         auto* cl_zero = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(FLAGS_write_size, 64));
         auto* cl_ones = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(FLAGS_write_size, 64));
         memset(cl_ones, 1, FLAGS_write_size);
         running_threads_counter++;
         uint64_t SIGNAL_ = 16 - 1;
         uint64_t ops = 0;
         uint64_t wqe = 0;
         while (keep_running) {
            if (!keep_running) break;
            size_t remote_addr = desc.start;
            rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
            if (ops % 2 == 0)
               rdma::postWrite(cl_zero, *rctx, signal, remote_addr, FLAGS_write_size);
            else
               rdma::postWrite(cl_ones, *rctx, signal, remote_addr, FLAGS_write_size);
            wqe++;
            ops++;
            writes++;
            // -------------------------------------------------------------------------------------
            if ((wqe & SIGNAL_) == SIGNAL_) {
               int comp{0};
               ibv_wc wcReturn;
               while (comp == 0) {
                  comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
               }
            }
         }
         running_threads_counter--;
      });
      // -------------------------------------------------------------------------------------
      // Reader
      for (uint64_t t_i = 1; t_i < FLAGS_worker; ++t_i) {
         compute.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            auto& cm = compute.getCM();
            auto* rctx = threads::Worker::my().cctxs[0].rctx;
            auto desc = threads::Worker::my().catalog[0];
            auto* buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(FLAGS_write_size, 64));
            uint8_t cl_ones[FLAGS_write_size];
            uint8_t cl_zeros[FLAGS_write_size];
            memset(&cl_ones, 1, FLAGS_write_size);
            memset(&cl_zeros, 0, FLAGS_write_size);
            running_threads_counter++;
            while (keep_running) {
               size_t remote_addr = desc.start;
               rdma::postRead(buffer, *rctx, rdma::completion::signaled, remote_addr, FLAGS_write_size, 0);
               // -------------------------------------------------------------------------------------
               int comp{0};
               ibv_wc wcReturn;
               while (comp == 0) {
                  comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
               }
               auto ones = memcmp(buffer, cl_ones, FLAGS_write_size);
               auto zeros = memcmp(buffer, cl_zeros, FLAGS_write_size);

               
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
               if (ones != 0 && zeros != 0) { throw std::runtime_error("torn writes"); }
               if (ones == 0 && zeros == 0) { throw std::runtime_error("torn writes"); }
            }
            running_threads_counter--;
         });
      }
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
