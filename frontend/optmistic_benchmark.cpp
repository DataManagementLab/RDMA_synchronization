#include "BenchmarkHelper.hpp"
#include "Defs.hpp"
#include "OptimisticLocks.hpp"
#include "PerfEvent.hpp"
#include "exception_hack.hpp"
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
#include <charconv>
#include "nam/utils/crc64.hpp"
// -------------------------------------------------------------------------------------

DEFINE_double(run_for_seconds, 10.0, "");
DEFINE_uint64(lock_count, 16, "");

DEFINE_uint64(block_size, 512, "");
DEFINE_bool(footer, false, "");
DEFINE_bool(versioning, false, "");
DEFINE_bool(CRC, false, "");
DEFINE_bool(farm, false, "");
DEFINE_bool(broken, false, "");
DEFINE_bool(pessimistic, false, "");
DEFINE_uint64(padding, 8, "");
DEFINE_uint64(sleep, 0, "sleep in microseconds ");
DEFINE_string(zipfs, "0", "zipfs format must be space delimited and integer scaled 100 -> 1.0");
DEFINE_string(readratios, "100 50 0", "zipfs format must be space delimited");


static std::vector<unsigned> interpretGflagString(std::string_view desc) {
   std::vector<unsigned> splitted;
   auto add = [&](std::string_view desc) {
      unsigned c = 0;
      std::from_chars(desc.data(), desc.data() + desc.length(), c);
      splitted.push_back(c);
   };
   while (desc.find(' ') != std::string_view::npos) {
      auto split = desc.find(' ');
      add(desc.substr(0, split));
      desc = desc.substr(split + 1);
   }
   add(desc);
   return splitted;
}


template <typename Consistency, typename LockType>
void run_test(uint32_t READ_RATIO,
              nam::rdma::RdmaContext& rctx,
              uintptr_t lock_addr,
              uint64_t* lock_buffer,
              uint64_t* tuple_buffer,
              uint64_t& updates,
              uint64_t& reads,
              uint64_t& aborts) {
   if (READ_RATIO == 100 || utils::RandomGenerator::getRandU64(0, 100) < READ_RATIO) {
      auto start = utils::getTimePoint();
      OptimisticLock<Consistency, LockType> tuple(rctx, lock_addr, tuple_buffer, FLAGS_block_size);
      for (uint64_t repeatCounter = 0;; repeatCounter++) {
         try {
            tuple.lock();
            tuple.unlock();
            break;
         } catch (const OLRestartException&) {
            threads::Worker::my().counters.incr(profiling::WorkerCounters::abort);
            aborts++;
         }
      }
      reads++;
      auto end = utils::getTimePoint();
      threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
   } else {
      auto rnd_cl = utils::RandomGenerator::getRandU64(0, (FLAGS_block_size / CL));
      auto rnd_idx = utils::RandomGenerator::getRandU64(3, 6);
      auto index = rnd_cl * (CL / sizeof(uint64_t)) + rnd_idx;

      auto start = utils::getTimePoint();
      ExclusiveLock<Consistency, LockType> tuple(rctx, lock_addr, lock_buffer, tuple_buffer, FLAGS_block_size);
      for (uint64_t repeatCounter = 0;; repeatCounter++) {
         try {
            tuple.lock();
            tuple_buffer[index] += 5;
            tuple.unlock();
            break;
         } catch (const OLRestartException&) {
            threads::Worker::my().counters.incr(profiling::WorkerCounters::abort);
            aborts++;
         }
      }
      auto end = utils::getTimePoint();
      updates++;
      threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
   }
}

void run_pessimistic(uint32_t READ_RATIO,
                     nam::rdma::RdmaContext& rctx,
                     uintptr_t lock_addr,
                     uint64_t* lock_buffer,
                     uint64_t* tuple_buffer,
                     uint64_t& updates,
                     uint64_t& reads,
                     uint64_t& aborts) {
   if (READ_RATIO == 100 || utils::RandomGenerator::getRandU64(0, 100) < READ_RATIO) {
      auto start = utils::getTimePoint();
      ReaderWriterLock tuple(rctx, lock_addr, lock_buffer, tuple_buffer, FLAGS_block_size);
      for (uint64_t repeatCounter = 0;; repeatCounter++) {
         try {
            tuple.lockShared();
            tuple.unlockShared();
            break;
         } catch (const OLRestartException&) {
            threads::Worker::my().counters.incr(profiling::WorkerCounters::abort);
            aborts++;
         }
      }
      reads++;
      auto end = utils::getTimePoint();
      threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
   } else {
      auto rnd_cl = utils::RandomGenerator::getRandU64(0, (FLAGS_block_size / CL));
      auto rnd_idx = utils::RandomGenerator::getRandU64(3, 6);
      auto index = rnd_cl * (CL / sizeof(uint64_t)) + rnd_idx;

      auto start = utils::getTimePoint();
      ReaderWriterLock tuple(rctx, lock_addr, lock_buffer, tuple_buffer, FLAGS_block_size);
      for (uint64_t repeatCounter = 0;; repeatCounter++) {
         try {
            tuple.lockExclusive();
            tuple_buffer[index] += 5;
            tuple.unlockExclusive();
            break;
         } catch (const OLRestartException&) {
            threads::Worker::my().counters.incr(profiling::WorkerCounters::abort);
            aborts++;
         }
      }
      auto end = utils::getTimePoint();
      updates++;
      threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
   }
}



template <typename Consistency, typename LockType>
void run_check(uint32_t READ_RATIO,
               nam::rdma::RdmaContext& rctx,
               uintptr_t lock_addr,
               uint64_t* lock_buffer,
               uint64_t* tuple_buffer,
               uint64_t& updates,
               uint64_t& reads,
               uint64_t& aborts) {
   if (READ_RATIO == 100 || utils::RandomGenerator::getRandU64(0, 100) < READ_RATIO) {
      auto start = utils::getTimePoint();
      OptimisticLock<Consistency, LockType> tuple(rctx, lock_addr, tuple_buffer, FLAGS_block_size);
      for (uint64_t repeatCounter = 0;; repeatCounter++) {
         try {
            tuple.lock();
            // go over the entries
            tuple.unlock();
            // after the validation we have a consistent snapshot
            // -------------------------------------------------------------------------------------
            uint64_t prev = tuple_buffer[3];
            for (uint64_t cl_i = 0; cl_i < (FLAGS_block_size / CL); cl_i++) {
               for (uint64_t v_i = 3; v_i < 6; v_i++) {
                  auto idx = (cl_i * 8) + v_i;
                  if (prev != tuple_buffer[idx]) {
                     std::cout << "not equal"
                               << "\n";
                  }
               }
            }
            // -------------------------------------------------------------------------------------
            break;
         } catch (const OLRestartException&) {
            threads::Worker::my().counters.incr(profiling::WorkerCounters::abort);
            aborts++;
         }
      }
      reads++;
      auto end = utils::getTimePoint();
      threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
   } else {
      auto start = utils::getTimePoint();
      ExclusiveLock<Consistency, LockType> tuple(rctx, lock_addr, lock_buffer, tuple_buffer, FLAGS_block_size);
      for (uint64_t repeatCounter = 0;; repeatCounter++) {
         try {
            tuple.lock();
            // increment the values
            for (uint64_t cl_i = 0; cl_i < (FLAGS_block_size / CL); cl_i++) {
               for (uint64_t v_i = 3; v_i < 6; v_i++) {
                  auto idx = (cl_i * 8) + v_i;
                  tuple_buffer[idx]++;
               }
            }

            tuple.unlock();
            break;
         } catch (const OLRestartException&) {
            threads::Worker::my().counters.incr(profiling::WorkerCounters::abort);
            aborts++;
         }
      }
      auto end = utils::getTimePoint();
      updates++;
      threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
   }
}

int main(int argc, char* argv[]) {
   exception_hack::init_phdr_cache();
   // -------------------------------------------------------------------------------------
   gflags::SetUsageMessage("Storage-DB Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   using namespace nam;

   if (FLAGS_storage_node) {
      ensure(((FLAGS_lock_count * FLAGS_block_size) + (FLAGS_lock_count * FLAGS_padding)) < (FLAGS_dramGB * 1024 * 1024 * 1024));
      std::cout << "Storage Node" << std::endl;
      nam::Storage db;
      db.registerMemoryRegion("block", FLAGS_dramGB * 1024 * 1024 * 1024);
      db.startAndConnect();
      // -------------------------------------------------------------------------------------
      while (db.getCM().getNumberIncomingConnections()) {}
      // auto desc = db.getMemoryRegion("block");
      // uint8_t* buffer = (uint8_t*)desc.start +64;
      // for(uint64_t t_i = 0; t_i < FLAGS_lock_count; t_i++){
      //    auto lock_addr = (uint64_t*)(buffer + (t_i * FLAGS_block_size) + (t_i * FLAGS_padding));
      //    for (uint64_t i = 1; i < FLAGS_block_size / sizeof(uint64_t); ++i) {
      //          std::cout << "  " << lock_addr[i];
      //    }
      //    std::cout << " " << "\n";
      // }
   } else {
      nam::Compute compute;
      std::string benchmark = "optimistic";

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
      if (FLAGS_pessimistic) { benchmark = "pessimistic"; }
      // -------------------------------------------------------------------------------------
      std::vector<std::string> workload_type;  // warm up or benchmark
      std::vector<double> zipfs;
      auto workloads = interpretGflagString(FLAGS_readratios);
      {
         auto tmp_zipf = interpretGflagString(FLAGS_zipfs);
         std::cout << "input size " << tmp_zipf.size() << "\n";

         for(auto tz : tmp_zipf){
            zipfs.push_back(tz/(double)100);
         }
         for(auto z : zipfs){
            std::cout <<" zipfs " << z << "\n";
         }

      }

      // -------------------------------------------------------------------------------------
      u64 lock_count = FLAGS_lock_count;
      // -------------------------------------------------------------------------------------
      std::atomic<uint64_t> g_updates = 0;
      std::atomic<uint64_t> g_aborts = 0;
      std::atomic<uint64_t> g_reads = 0;
      uint64_t stage = 1;
      // -------------------------------------------------------------------------------------
      crc64_init();
      // -------------------------------------------------------------------------------------
      for (auto ZIPF : zipfs) {
         std::unique_ptr<utils::ScrambledZipfGenerator> zipf_random;
         zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, lock_count, ZIPF);
         // -------------------------------------------------------------------------------------
         for (auto READ_RATIO : workloads) {
            std::atomic<bool> keep_running = true;
            std::atomic<u64> running_threads_counter = 0;
            benchmark::OPT_LOCKING_BENCHMARK_workloadInfo experimentInfo{
                benchmark, FLAGS_lock_count, READ_RATIO, ZIPF, std::to_string(FLAGS_padding), FLAGS_block_size};
            compute.startProfiler(experimentInfo);
            for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
               compute.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
                  uint64_t updates = 0;
                  uint64_t aborts = 0;
                  uint64_t reads = 0;
                  auto& cm = compute.getCM();
                  auto* rctx = threads::Worker::my().cctxs[0].rctx;
                  auto desc = threads::Worker::my().catalog[0];
                  std::vector<uint64_t*> tuple_buffers;
                  std::vector<uint64_t*> lock_buffers;
                  tuple_buffers.push_back(static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(FLAGS_block_size, 64)));
                  tuple_buffers.push_back(static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(FLAGS_block_size, 64)));
                  // -------------------------------------------------------------------------------------
                  lock_buffers.push_back(static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64)));
                  lock_buffers.push_back(static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64)));
                  // -------------------------------------------------------------------------------------
                  // -------------------------------------------------------------------------------------
                  uint64_t* barrier_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
                  auto addr = desc.start + 64;
                  // -------------------------------------------------------------------------------------
                  auto barrier_addr = desc.start;
                  rdma_barrier_wait(barrier_addr, stage, barrier_buffer, *rctx);
                  // -------------------------------------------------------------------------------------
                  // WRITE
                  // -------------------------------------------------------------------------------------
                  running_threads_counter++;
                  uint64_t b = 0;
                  while (keep_running) {
                     uint64_t lock_id = zipf_random->rand(0);
                     auto lock_addr = addr + (lock_id * FLAGS_block_size) + (lock_id * FLAGS_padding);
                     ensure(lock_id < lock_count);

                     if (FLAGS_pessimistic) {
                        run_pessimistic(READ_RATIO, *rctx, lock_addr, lock_buffers[b % 2], tuple_buffers[b % 2], updates, reads, aborts);
                     } else {
                        if (FLAGS_footer) {
                           if (FLAGS_versioning) {
                              run_test<V2, FooterLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[b % 2], tuple_buffers[b % 2], updates,
                                                       reads, aborts);
                           } else if (FLAGS_CRC) {
                              run_test<CRC, FooterLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[b % 2], tuple_buffers[b % 2], updates,
                                                        reads, aborts);
                           } else if (FLAGS_farm) {
                              run_test<FaRM, FooterLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[b % 2], tuple_buffers[b % 2], updates,
                                                         reads, aborts);
                           } else if (FLAGS_broken) {
                              run_test<Broken, FooterLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[b % 2], tuple_buffers[b % 2], updates,
                                                           reads, aborts);
                           } else
                              throw std::runtime_error("wrong option");
                        } else {
                           if (FLAGS_versioning) {
                              run_test<V2, HeaderLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[b % 2], tuple_buffers[b % 2], updates,
                                                       reads, aborts);
                           } else if (FLAGS_CRC) {
                              run_test<CRC, HeaderLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[b % 2], tuple_buffers[b % 2], updates,
                                                        reads, aborts);

                           } else if (FLAGS_farm) {
                              run_test<FaRM, HeaderLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[b % 2], tuple_buffers[b % 2], updates,
                                                         reads, aborts);
                           }else if (FLAGS_broken) {
                              run_test<Broken, FooterLock>(READ_RATIO, *rctx, lock_addr, lock_buffers[b % 2], tuple_buffers[b % 2], updates,
                                                           reads, aborts);
                           }
                        }
                     }
                     b++;
                     threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
                  }
                  g_updates += updates;
                  g_aborts += aborts;
                  g_reads += reads;
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
            stage++;
         }
         std::cout << "updates " << g_updates << "\n";
         std::cout << "reads " << g_reads << "\n";
         std::cout << "aborts " << g_aborts << "\n";
      }
   }
   return 0;
}
