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
DEFINE_bool(record_latency, false, "");
DEFINE_uint64(alignment,8,"");
DEFINE_uint64(working_size,8,"number of atomics ");

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
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      std::string benchmark = "alignment";
      benchmark::ALIGNMENT_WS_workloadInfo experimentInfo{benchmark, FLAGS_alignment, FLAGS_working_size};
      compute.startProfiler(experimentInfo);
      compute.getWorkerPool().scheduleJobAsync(0, [&]() {
         running_threads_counter++;
         auto& cm = compute.getCM();
         auto* rctx = threads::Worker::my().cctxs[0].rctx;
         auto desc = threads::Worker::my().catalog[0];
         auto* tl_rdma_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
         // uint64_t desired = 0;
         // uint64_t expected = 0;
         // uint64_t wqe = 0;
         // uint64_t SIGNAL_ = 32 -1;
         // -------------------------------------------------------------------------------------
         // create write batch
         
         auto* qp = rctx->id->qp;
         auto* mr=  rctx->mr;
         auto rkey =   rctx->rkey;

         constexpr size_t batch = 16;
         struct ibv_send_wr sq_wr[batch];
         struct ibv_sge send_sgl[batch];
         struct ibv_send_wr* bad_wr;
         for (uint64_t b_i = 0; b_i < batch; b_i++) {
            send_sgl[b_i].addr = (uintptr_t)tl_rdma_buffer;
            send_sgl[b_i].length = 8;
            send_sgl[b_i].lkey = mr->lkey;
            sq_wr[b_i].opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
            sq_wr[b_i].send_flags = ((b_i == batch -1) ) ? IBV_SEND_SIGNALED : 0;
            sq_wr[b_i].wr.atomic.remote_addr    = desc.start + ((b_i % FLAGS_working_size) * FLAGS_alignment);
            sq_wr[b_i].wr.atomic.rkey           = rkey;
            sq_wr[b_i].wr.atomic.compare_add    = 1; /* value to be added to the remote address content */
            sq_wr[b_i].sg_list = &send_sgl[b_i];
            sq_wr[b_i].num_sge = 1;
            sq_wr[b_i].wr_id = 0; 
            sq_wr[b_i].next = (b_i == batch -1 ) ? nullptr : &sq_wr[b_i+1];  // do not forget to set this otherwise it  crashes
         }
         
         // -------------------------------------------------------------------------------------
         int budget = 4;
         while (keep_running) {            
               
            for (;budget > 0; budget--) {
               auto start = utils::getTimePoint();
               auto ret = ibv_post_send(qp, &sq_wr[0], &bad_wr);
               if (ret)
                  throw std::runtime_error("Failed to post send request" + std::to_string(ret) + " " + std::to_string(errno));               
               auto end = utils::getTimePoint();
               threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
            int comp{0};
            ibv_wc wcReturn;
            while (comp == 0) {
               comp = rdma::pollCompletion(rctx->id->qp->send_cq, 16, &wcReturn);
               budget += comp;
            }
         }
         running_threads_counter--;
      });
      
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
