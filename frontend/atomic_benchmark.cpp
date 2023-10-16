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
      benchmark::SYNC_workloadInfo experimentInfo{"atomics", inconsistencies, blocks_read, 0, 0};
      compute.startProfiler(experimentInfo);

      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         compute.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            running_threads_counter++;
            auto& cm = compute.getCM();
            auto* rctx = threads::Worker::my().cctxs[0].rctx;
            auto desc = threads::Worker::my().catalog[0];
            auto addr = desc.start;
            auto* tl_rdma_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
            uint64_t increment = 1;
            while (keep_running) {
               auto start = utils::getTimePoint();
               // add atomic operations
               auto* old = reinterpret_cast<uint64_t*>(tl_rdma_buffer);
               rdma::postFetchAdd(increment, old, *(rctx), rdma::completion::signaled, addr);
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
      if (FLAGS_record_latency) {
         std::atomic<bool> keep_running = true;
         constexpr uint64_t LATENCY_SAMPLES = 1e6;
         benchmark::SYNC_workloadInfo experimentInfo{"atomics", inconsistencies, blocks_read, 0, 0};
         compute.startProfiler(experimentInfo);
         std::vector<uint64_t> tl_microsecond_latencies[FLAGS_worker];
         std::vector<uint64_t> samples_taken(FLAGS_worker);
         for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
            compute.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
               running_threads_counter++;
               auto& cm = compute.getCM();
               auto* rctx = threads::Worker::my().cctxs[0].rctx;
               auto desc = threads::Worker::my().catalog[0];
               auto addr = desc.start;
               uint64_t ops = 0;
               auto* tl_rdma_buffer = static_cast<uint64_t*>(cm.getGlobalBuffer().allocate(64, 64));
               uint64_t increment=1;
               tl_microsecond_latencies[t_i].reserve(LATENCY_SAMPLES);
               while (keep_running) {
                  auto start = utils::getTimePointNanoseconds();
                  auto* old = reinterpret_cast<uint64_t*>(tl_rdma_buffer);
                  rdma::postFetchAdd(increment, old, *(rctx), rdma::completion::signaled, addr);
                  int comp{0};
                  ibv_wc wcReturn;
                  while (comp == 0) {
                     comp = rdma::pollCompletion(rctx->id->qp->send_cq, 1, &wcReturn);
                     if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
                  }
                  auto end = utils::getTimePointNanoseconds();
                  threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                  if (ops < LATENCY_SAMPLES) tl_microsecond_latencies[t_i].push_back(end - start);
                  threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
                  ops++;
               }
               samples_taken[t_i] = ops;
               running_threads_counter--;
            });
         }
         sleep(FLAGS_run_for_seconds);
         keep_running = false;
         while (running_threads_counter) {
            _mm_pause();
         }
         // -------------------------------------------------------------------------------------
         // Join Threads
         // -------------------------------------------------------------------------------------
         compute.getWorkerPool().joinAll();
         // -------------------------------------------------------------------------------------
         compute.stopProfiler();
         // -------------------------------------------------------------------------------------
         // combine vector of threads into one
         std::vector<uint64_t> microsecond_latencies;
         for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
            microsecond_latencies.insert(microsecond_latencies.end(), tl_microsecond_latencies[t_i].begin(),
                                         tl_microsecond_latencies[t_i].end());
         }

         {
            std::cout << "Shuffle samples " << microsecond_latencies.size() << std::endl;
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(microsecond_latencies.begin(), microsecond_latencies.end(), g);

            // write out 400 samples
            std::ofstream latency_file;
            std::ofstream::openmode open_flags = std::ios::app;
            std::string filename = "latency_samples_" + FLAGS_csvFile;
            bool csv_initialized = std::filesystem::exists(filename);
            latency_file.open(filename, open_flags);
            if (!csv_initialized) { latency_file << "workload,worker,latency" << std::endl; }
            for (uint64_t s_i = 0; s_i < 1000; s_i++) {
               latency_file << "atomic"
                            << "," << FLAGS_worker << "," << microsecond_latencies[s_i] << std::endl;
            }
            latency_file.close();
         }
         std::cout << "Sorting Latencies"
                   << "\n";
         std::sort(microsecond_latencies.begin(), microsecond_latencies.end());
         std::cout << "Latency (min/median/max/99%): " << (microsecond_latencies[0]) << ","
                   << (microsecond_latencies[microsecond_latencies.size() / 2]) << "," << (microsecond_latencies.back()) << ","
                   << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.99)]) << std::endl;
         // -------------------------------------------------------------------------------------
         // write to csv file
         std::ofstream latency_file;
         std::ofstream::openmode open_flags = std::ios::app;
         std::string filename = "latency_" + FLAGS_csvFile;
         bool csv_initialized = std::filesystem::exists(filename);
         latency_file.open(filename, open_flags);
         if (!csv_initialized) { latency_file << "workload,workers,min,median,max,95th,99th,999th" << std::endl; }
         latency_file << "atomic"
                      << "," << FLAGS_worker << "," << (microsecond_latencies[0]) << ","
                      << (microsecond_latencies[microsecond_latencies.size() / 2]) << "," << (microsecond_latencies.back()) << ","
                      << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.95)]) << ","
                      << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.99)]) << ","
                      << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.999)]) << std::endl;
         latency_file.close();
      }
   }
   return 0;
}
