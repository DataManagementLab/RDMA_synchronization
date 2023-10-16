#include "BenchmarkHelper.hpp"
#include "Defs.hpp"
#include "PerfEvent.hpp"
#include "nam/Compute.hpp"
#include "nam/Config.hpp"
#include "nam/Storage.hpp"
#include "OptimisticLocks.hpp"
#include "exception_hack.hpp"
#include "nam/NAM.hpp"
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
DEFINE_uint64(padding, 8, "");
DEFINE_bool(footer, false, "");
DEFINE_bool(versioning, false, "");
DEFINE_bool(CRC, false, "");
DEFINE_bool(farm, false, "");
DEFINE_bool(broken, false, "");


// static constexpr uint64_t TUPLE_SIZE = 256;  // spans multiple cl to get the correctness.
static constexpr uint64_t TUPLE_SIZE = 512;  // spans multiple cl to get the correctness.

template <typename Consistency, typename LockType>
void run_check(uint32_t READ_RATIO, nam::rdma::RdmaContext& rctx, uintptr_t lock_addr, uint64_t* lock_buffer, uint64_t* tuple_buffer) {
   if (READ_RATIO == 100 || utils::RandomGenerator::getRandU64(0, 100) < READ_RATIO) {
      auto start = utils::getTimePoint();
      OptimisticLock<Consistency, LockType> tuple(rctx, lock_addr, tuple_buffer, TUPLE_SIZE);
      for (uint64_t repeatCounter = 0;; repeatCounter++) {
         try {
            tuple.lock();
            // go over the entries
            [[maybe_unused]] auto versions = tuple.unlock();
            // after the validation we have a consistent snapshot
            // -------------------------------------------------------------------------------------
            uint64_t prev = tuple_buffer[3];
            bool corrupt = false;
            for (uint64_t cl_i = 0; cl_i < (TUPLE_SIZE / CL); cl_i++) {
               for (uint64_t v_i = 3; v_i < 6; v_i++) {
                  auto idx = (cl_i * 8) + v_i;
                  if (prev != tuple_buffer[idx]) {
                     corrupt = true;
                  }
               }
            }
            if(corrupt){
               for (uint64_t cl_i = 0; cl_i < (TUPLE_SIZE / CL); cl_i++) {
                  for (uint64_t v_i = 0; v_i < 8; v_i++) {
                     auto idx = (cl_i * 8) + v_i;
                     std::cout <<  tuple_buffer[idx] << "  ";
                  }
               }
               std::cout << " "  << std::endl;
               std::cout << "versions " << versions.first << " " << versions.second << std::endl;
               throw std::runtime_error("Read consistency check failed ");
            }
            // -------------------------------------------------------------------------------------
            break;
         } catch (const OLRestartException&) {
            threads::Worker::my().counters.incr(profiling::WorkerCounters::abort);
         }
      }
      auto end = utils::getTimePoint();
      threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
   } else {
      auto start = utils::getTimePoint();
      ExclusiveLock<Consistency, LockType> tuple(rctx, lock_addr, lock_buffer, tuple_buffer, TUPLE_SIZE);
      for (uint64_t repeatCounter = 0;; repeatCounter++) {
         try {
            tuple.lock();
            // increment the values
            for (uint64_t cl_i = 0; cl_i < (TUPLE_SIZE / CL); cl_i++) {
               for (uint64_t v_i = 3; v_i < 6; v_i++) {
                  auto idx = (cl_i * 8) + v_i;
                  tuple_buffer[idx]++;
               }
            }
            // uint64_t prev = tuple_buffer[3];
            // bool corrupt = false;
            // for (uint64_t cl_i = 0; cl_i < (TUPLE_SIZE / CL); cl_i++) {
            //    for (uint64_t v_i = 3; v_i < 6; v_i++) {
            //       auto idx = (cl_i * 8) + v_i;
            //       if (prev != tuple_buffer[idx]) {
            //          corrupt = true;
            //       }
            //    }
            // }
            // if(corrupt){
            //    for (uint64_t cl_i = 0; cl_i < (TUPLE_SIZE / CL); cl_i++) {
            //       for (uint64_t v_i = 0; v_i < 8; v_i++) {
            //          auto idx = (cl_i * 8) + v_i;
            //          std::cout <<  tuple_buffer[idx] << "  ";
            //       }
            //    }
            //    std::cout << " "  << std::endl;
            //    throw std::runtime_error("Write consistency check failed ");

            //    if((prev -1) != tuple_buffer[((TUPLE_SIZE/sizeof(uint64_t))-1)]){
            //       throw std::runtime_error("Write consistency check failed ");
            //    }
            // }

            tuple.unlock();
            break;
         } catch (const OLRestartException&) {
            threads::Worker::my().counters.incr(profiling::WorkerCounters::abort);
         }
      }
      auto end = utils::getTimePoint();
      threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
   }
}

int main(int argc, char* argv[]) {
   exception_hack::init_phdr_cache();
   gflags::SetUsageMessage("Storage-DB Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   using namespace nam;
   nam::NAM db;
   db.registerMemoryRegion("block", FLAGS_dramGB * 1024 * 1024 * 1024);
   // -------------------------------------------------------------------------------------
   // colocated nam-db 
   db.startAndConnect();
   // -------------------------------------------------------------------------------------
   std::string benchmark = "OPTDB";
   if (FLAGS_versioning) {
      benchmark += "-versioning";
   } else if (FLAGS_CRC) {
      benchmark += "-CRC";
   } else if (FLAGS_farm) {
      benchmark += "-FaRM";
   } else if (FLAGS_broken) {
      benchmark += "-broken";
   }
   if (FLAGS_footer) { benchmark += "-footer"; }
   // -------------------------------------------------------------------------------------
   std::vector<std::string> workload_type;  // warm up or benchmark
   std::vector<uint32_t> workloads;
   std::vector<double> zipfs;
   workloads.push_back(100);
   workloads.push_back(50);
   workloads.push_back(0);
   zipfs.insert(zipfs.end(), {0,1,1.5,2,2.5});
   // zipfs.insert(zipfs.end(), {1.5,2});
   // -------------------------------------------------------------------------------------
   u64 lock_count = FLAGS_lock_count;
   // -------------------------------------------------------------------------------------
   crc64_init();
   // -------------------------------------------------------------------------------------
   std::atomic<uint64_t> g_updates = 0;
   uint64_t stage =1;
   for (auto ZIPF : zipfs) {
      std::unique_ptr<utils::ScrambledZipfGenerator> zipf_random;
      zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, lock_count, ZIPF);
      // -------------------------------------------------------------------------------------
      for (auto READ_RATIO : workloads) {
         std::atomic<bool> keep_running = true;
         std::atomic<u64> running_threads_counter = 0;
         nam::concurrency::Barrier b(FLAGS_worker+1); //include main thread 
         benchmark::LOCKING_BENCHMARK_workloadInfo experimentInfo{benchmark, FLAGS_lock_count, READ_RATIO, ZIPF,
                                                                  std::to_string(FLAGS_padding)};
         db.startProfiler(experimentInfo);
         for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
            db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
               uint64_t updates = 0;
               auto& cm = db.getCM();
               uint64_t* barrier_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
               std::vector<uint64_t*> tuple_buffers(FLAGS_storage_nodes);
               std::vector<uint64_t*> lock_buffers(FLAGS_storage_nodes);

               for(uint64_t s_i =0; s_i < FLAGS_storage_nodes; s_i++){
                  tuple_buffers[s_i] = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(TUPLE_SIZE, 64));
                  lock_buffers[s_i] = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
               }
               auto& cctxs = threads::Worker::my().cctxs;
               auto& catalog = threads::Worker::my().catalog;
               auto barrier_addr = catalog[0].start;
               rdma_barrier_nam_wait(barrier_addr,stage,barrier_buffer, *cctxs[0].rctx );
               b.wait();
               uint64_t current_node =0;
               running_threads_counter++;
               while (keep_running) {
                  uint64_t s_id = current_node % FLAGS_storage_nodes;
                  current_node++;
                  auto* rctx = cctxs[s_id].rctx;
                  auto desc = catalog[s_id];
                  auto addr = desc.start + 64;
                  uint64_t lock_id = zipf_random->rand(0);
                  auto lock_addr = addr + (lock_id * TUPLE_SIZE) + (lock_id * FLAGS_padding);
                  // todo copy form opt benchmark just adjust number nodes and then run_check
                  if (FLAGS_footer) {
                     if (FLAGS_versioning) {
                        run_check<V2, FooterLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[s_id], tuple_buffers[s_id]);
                     } else if (FLAGS_CRC) {
                        run_check<CRC, FooterLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[s_id], tuple_buffers[s_id]);
                     } else if (FLAGS_farm) {
                        run_check<FaRM, FooterLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[s_id], tuple_buffers[s_id]);
                     } else if (FLAGS_broken) {
                        run_check<Broken, FooterLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[s_id], tuple_buffers[s_id]);
                     } else
                        throw std::runtime_error("wrong option");
                  } else {
                     if (FLAGS_versioning) {
                        run_check<V2, HeaderLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[s_id], tuple_buffers[s_id]);
                     } else if (FLAGS_CRC) {
                        run_check<CRC, HeaderLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[s_id], tuple_buffers[s_id]);

                     } else if (FLAGS_farm) {
                        run_check<FaRM, HeaderLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[s_id], tuple_buffers[s_id]);
                     }else if (FLAGS_broken) {
                        run_check<Broken, FooterLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[s_id], tuple_buffers[s_id]);
                     }
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
         b.wait();
         sleep(FLAGS_run_for_seconds);
         keep_running = false;
         while (running_threads_counter) {
            _mm_pause();
         }
         db.getWorkerPool().joinAll();
         // -------------------------------------------------------------------------------------
         db.stopProfiler();
         stage++;
      }
      std::cout << "updates " << g_updates << "\n";
   }
   return 0;
}
