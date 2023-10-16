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
#include "nam/utils/crc64.hpp"
#include "BenchmarkHelper.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
#include <fcntl.h>
// -------------------------------------------------------------------------------------


DEFINE_double(run_for_seconds, 10.0, "");
DEFINE_uint64(block_size, 512 ,"");
DEFINE_bool(versioning, false, "");
DEFINE_bool(CRC, false, "");
DEFINE_bool(pessimistic, false, "");
DEFINE_bool(farm, false, "");


static constexpr uint64_t EXCLUSIVE_LOCKED = 0x1000000000000000;
static constexpr uint64_t EXCLUSIVE_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF - EXCLUSIVE_LOCKED + 1;
static constexpr uint64_t UNLOCKED = 0;
static constexpr uint64_t MASKED_SHARED_LOCKS = 0x1000000000000000;
static constexpr uint64_t SHARED_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF;

typedef unsigned long long ticks;
static constexpr bool PROFILING = true;

// Taken from stackoverflow (see http://stackoverflow.com/questions/3830883/cpu-cycle-count-based-profiling-in-c-c-linux-x86-64)
// Can give nonsensical results on multi-core AMD processors.
ticks rdtsc() {
    unsigned int lo, hi;
    asm volatile (
        "cpuid \n" /* serializing */
        "rdtsc"
        : "=a"(lo), "=d"(hi) /* outputs */
        : "a"(0) /* inputs */
        : "%ebx", "%ecx");
    /* clobbers*/
    return ((unsigned long long) lo) | (((unsigned long long) hi) << 32);
}

ticks startRDTSC(void) {
    return rdtsc();
}

ticks stopRDTSCP(void) {
    return rdtsc();
}

// Tuple layout 
// cl versions
// CRC is idx 1 uint64_t in tuple

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
      // initialize random tuple size so that it will work for all techniques
      int randomData = open("/dev/urandom", O_RDONLY);
      if (randomData < 0)
      {
         throw std::runtime_error("error /dev/urandom");
      }
      else
      {
         ssize_t result = read(randomData, buffer, FLAGS_block_size);
         if (result < 0)
         {
            throw std::runtime_error("error fill random");
         }
      }
      // set farm versions
      for (uint64_t i = 0; i < FLAGS_block_size / sizeof(uint64_t); i = i+8) {
         buffer[i] = 99;
      }

      crc64_init();
      buffer[1] = crc64(0x42F0E1EBA9EA3693, (unsigned char*)(&buffer[2]), FLAGS_block_size-16);
      std::cout << "created CRC " << buffer[1] << "\n";

      for (uint64_t i = 0; i < FLAGS_block_size / sizeof(uint64_t); i++) {
         std::cout << buffer[i] << "\n";
      }
      
      db.startAndConnect();
      // -------------------------------------------------------------------------------------
      while(db.getCM().getNumberIncomingConnections()){
      }

   } else {
      std::cout << "Compute Node" << std::endl;
      nam::Compute compute;
      std::string benchmark = "optimistic";

      if(FLAGS_versioning){
         benchmark += "-versioning";
      }else if (FLAGS_CRC){
         benchmark += "-CRC";
      }else if (FLAGS_farm){
         benchmark += "-FaRM";
      }
      else if (FLAGS_pessimistic){
         benchmark = "pessimistic";
      }

      benchmark::Opt_workloadInfo experimentInfo{benchmark, "read", FLAGS_block_size};
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
         auto* buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(FLAGS_block_size * 2, 64));
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
         // -------------------------------------------------------------------------------------
         crc64_init();
         // -------------------------------------------------------------------------------------
         [[maybe_unused]] ticks total_cycles = 0;
         [[maybe_unused]] ticks current = 0;
         [[maybe_unused]] int n= 0;
         while (keep_running) {
            // -------------------------------------------------------------------------------------
            // CAS for locking
            // -------------------------------------------------------------------------------------
            auto* old = reinterpret_cast<uint64_t*>(buffer);
            // implement logic here
            if(FLAGS_versioning){

               // read V
               rdma::postRead(old, *rctx, rdma::completion::unsignaled, addr, 8, 0);
               // read data 
               rdma::postRead(&old[1], *rctx, rdma::completion::signaled, addr+8, FLAGS_block_size-8 , 0);
               // -------------------------------------------------------------------------------------
               pollCQ();
               // -------------------------------------------------------------------------------------
               // read V to validate
               uint64_t prev = old[0];
               rdma::postRead(old, *rctx, rdma::completion::signaled, addr, 8, 0);
               pollCQ();
               if(prev != old[0])
                  throw std::runtime_error("Versions not equal");
               // -------------------------------------------------------------------------------------
            }else if (FLAGS_CRC){
               // -------------------------------------------------------------------------------------
               rdma::postRead(old, *rctx, rdma::completion::signaled, addr, FLAGS_block_size , 0);
               // -------------------------------------------------------------------------------------
               pollCQ();
               // -------------------------------------------------------------------------------------
               // uint64_t crc_v1 = crc64(0x42F0E1EBA9EA3693, (unsigned char*)old, FLAGS_block_size);
               uint64_t crc_v1 = crc64(0x42F0E1EBA9EA3693, (unsigned char*)(&old[2]), FLAGS_block_size-16);
               if(crc_v1 != old[1]){
                  throw std::runtime_error("CRC not equal");
               }
               // read v
               uint64_t prev = old[0];
               rdma::postRead(old, *rctx, rdma::completion::signaled, addr, 8, 0);
               pollCQ();
               if(prev != old[0])
                  throw std::runtime_error("Versions not equal");
               // -------------------------------------------------------------------------------------
            }else if (FLAGS_farm){
               // -------------------------------------------------------------------------------------
               // read D+V
               rdma::postRead(old, *rctx, rdma::completion::signaled, addr, FLAGS_block_size , 0);
               // -------------------------------------------------------------------------------------
               pollCQ();
               // -------------------------------------------------------------------------------------
               for (uint64_t i = 0; i < FLAGS_block_size / sizeof(uint64_t); i = i+8) {
                  if(old[i] != 99)
                     throw std::runtime_error("Versions not equal");
               }
               uint64_t prev = old[0];
               rdma::postRead(old, *rctx, rdma::completion::signaled, addr, 8, 0);
               pollCQ();
               if(prev != old[0])
                  throw std::runtime_error("Versions not equal");

            } else if (FLAGS_pessimistic) {
               // + speculative read
               auto speculative_read_s_lock = [&](uint64_t lock_addr) {
                  [[maybe_unused]] volatile uint64_t& s_locked = old[0];
                  rdma::postFetchAdd(1, old, *(rctx), rdma::completion::unsignaled, lock_addr);
                  // -------------------------------------------------------------------------------------
                  rdma::postRead(&old[1], *rctx, rdma::completion::signaled, lock_addr + 8, FLAGS_block_size - 8, 0);
                  pollCQ();
                  return true;
               };
               // + order_release
               auto s_order_release = [&](uint64_t lock_addr) {
                  rdma::postFetchAdd(int64_t{-1}, old, *(rctx), rdma::completion::unsignaled, lock_addr, true);
               };

               bool locked = false;
               locked = speculative_read_s_lock(addr);
               if (!locked) throw std::runtime_error("not possible");
               if( old[0] < 50){
                  throw std::runtime_error("not possible");
               }
               s_order_release(addr);
            } else {
               throw std::runtime_error("no option");
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
      std::cout << "" << std::endl;
   }
   return 0;
}
