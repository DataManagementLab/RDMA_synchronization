#pragma once
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
#include "nam/rdma/CommunicationManager.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
// -------------------------------------------------------------------------------------
namespace benchmark {
struct SYNC_workloadInfo : public nam::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t& inconsistencies;
   uint64_t& blocks_read;
   uint64_t block_size;
   uint64_t write_speed;
   uint64_t timestamp = 0;

   SYNC_workloadInfo(std::string experiment,
                     uint64_t& inconsistencies,
                     uint64_t& blocks_read,
                     uint64_t block_size,
                     uint64_t write_speed)
      : experiment(experiment),
        inconsistencies(inconsistencies),
        blocks_read(blocks_read),
        block_size(block_size),
        write_speed(write_speed)
      {}

   virtual std::vector<std::string> getRow() {
      return {
         experiment,
         std::to_string(inconsistencies),
         std::to_string(blocks_read),
         std::to_string(block_size),
         std::to_string(write_speed),
         std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader() {
      return {"workload", "inconsistencies", "blocks read", "block size" ,"write speed", "timestamp"};
   }

   virtual void csv(std::ofstream& file) override {
      file << experiment << " , ";
      file << inconsistencies << " , ";
      file << blocks_read << " , ";
      file << block_size << " , ";
      file << write_speed << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override {
      file << "Workload"
           << " , ";
      file << "Inconsistencies"
           << " , ";
      file << "BlocksRead"
           << " , ";
      file << "BlockSize"
           << " , ";
      file << "WriteSpeed"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};

struct LOCKING_workloadInfo : public nam::profiling::WorkloadInfo {
   std::string experiment;
   std::string operation;
   uint64_t timestamp = 0;

   LOCKING_workloadInfo(std::string experiment,
                     std::string operation)
      : experiment(experiment),
        operation(operation)
      {}

   virtual std::vector<std::string> getRow() {
      return {
         experiment,
         operation,
         std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader() {
      return {"workload", "operation", "timestamp"};
   }

   virtual void csv(std::ofstream& file) override {
      file << experiment << " , ";
      file << operation << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override {
      file << "Workload"
           << " , ";
      file << "Operation"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};



struct Opt_workloadInfo : public nam::profiling::WorkloadInfo {
   std::string experiment;
   std::string operation;
   std::uint64_t block_size;
   uint64_t timestamp = 0;

   Opt_workloadInfo(std::string experiment,
                        std::string operation,
                        std::uint64_t block_size)
      : experiment(experiment),
        operation(operation),
        block_size(block_size)
      {}

   virtual std::vector<std::string> getRow() {
      return {
         experiment,
         operation,
         std::to_string(block_size),
         std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader() {
      return {"workload", "operation", "BlockSize", "timestamp"};
   }

   virtual void csv(std::ofstream& file) override {
      file << experiment << " , ";
      file << operation << " , ";
      file << block_size << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override {
      file << "Workload"
           << " , ";
      file << "Operation"
           << " , ";
      file << "BlockSize"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};

struct LOCKING_BENCHMARK_workloadInfo : public nam::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t elements;
   uint64_t readRatio;
   double zipfFactor;
   std::string padding;
   uint64_t timestamp = 0;

   LOCKING_BENCHMARK_workloadInfo(std::string experiment, uint64_t elements, uint64_t readRatio, double zipfFactor, std::string padding)
      : experiment(experiment), elements(elements), readRatio(readRatio), zipfFactor(zipfFactor), padding(padding)
   {
   }

   
   virtual std::vector<std::string> getRow(){
      return {
          experiment, std::to_string(elements),    std::to_string(readRatio), std::to_string(zipfFactor),
          padding, std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader(){
      return {"workload","elements","read ratio", "zipfFactor", "padding", "timestamp"};
   }
   

   virtual void csv(std::ofstream& file) override
   {
      file << experiment << " , ";
      file << elements << " , ";
      file << readRatio << " , ";
      file << zipfFactor << " , ";
      file << padding << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override
   {
      file << "Workload"
           << " , ";
      file << "Elements"
           << " , ";
      file << "ReadRatio"
           << " , ";
      file << "ZipfFactor"
           << " , ";
      file << "padding"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};
// -------------------------------------------------------------------------------------


struct OPT_LOCKING_BENCHMARK_workloadInfo : public nam::profiling::WorkloadInfo {
   std::string optimistic;
   std::string pessimistic; 
   uint64_t elements;
   uint64_t readRatio;
   double zipfFactor;
   std::string padding;
   uint64_t block_size;
   uint64_t timestamp = 0;

   OPT_LOCKING_BENCHMARK_workloadInfo(std::string optimistic ,uint64_t elements, uint64_t readRatio, double zipfFactor, std::string padding, uint64_t block_size)
      : optimistic(optimistic), elements(elements), readRatio(readRatio), zipfFactor(zipfFactor), padding(padding), block_size(block_size)
   {
   }

   
   virtual std::vector<std::string> getRow(){
      return {
          optimistic, std::to_string(elements),    std::to_string(readRatio), std::to_string(zipfFactor),
          padding, std::to_string(block_size), std::to_string(timestamp++)
      };
   }

   virtual std::vector<std::string> getHeader(){
      return {"optimistic", "elements","read ratio", "zipfFactor", "padding", "blockSize", "timestamp"};
   }
   

   virtual void csv(std::ofstream& file) override
   {
      file << optimistic << " , ";
      file << elements << " , ";
      file << readRatio << " , ";
      file << zipfFactor << " , ";
      file << padding << " , ";
      file << block_size << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override
   {
      file << "Workload"
           << " , ";
      file << "Elements"
           << " , ";
      file << "ReadRatio"
           << " , ";
      file << "ZipfFactor"
           << " , ";
      file << "padding"
           << " , ";
      file << "BlockSize"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};
// -------------------------------------------------------------------------------------


// -------------------------------------------------------------------------------------

struct BATCHING_BENCHMARK_workloadInfo : public nam::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t elements;
   uint64_t batch;
   std::string padding;
   uint64_t timestamp = 0;

   BATCHING_BENCHMARK_workloadInfo(std::string experiment, uint64_t elements, uint64_t batch, std::string padding)
      : experiment(experiment), elements(elements), batch(batch), padding(padding)
   {
   }

   
   virtual std::vector<std::string> getRow(){
      return {
          experiment, std::to_string(elements),    std::to_string(batch),
          padding, std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader(){
      return {"workload","elements","batch", "padding", "timestamp"};
   }
   

   virtual void csv(std::ofstream& file) override
   {
      file << experiment << " , ";
      file << elements << " , ";
      file << batch << " , ";
      file << padding << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override
   {
      file << "Workload"
           << " , ";
      file << "Elements"
           << " , ";
      file << "batch"
           << " , ";
      file << "padding"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};
// -------------------------------------------------------------------------------------

struct CONTENTION_R_A_workloadInfo : public nam::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t elements;
   uint64_t reader;
   uint64_t writer;
   std::string padding;
   uint64_t timestamp = 0;

   CONTENTION_R_A_workloadInfo(std::string experiment, uint64_t elements, uint64_t reader, uint64_t writer, std::string padding)
      : experiment(experiment), elements(elements), reader(reader), writer(writer), padding(padding)
   {
   }

   
   virtual std::vector<std::string> getRow(){
      return {
         experiment, std::to_string(elements), std::to_string(reader), std::to_string(writer), padding, std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader(){
      return {"workload","elements","reader","writer" , "padding", "timestamp"};
   }
   

   virtual void csv(std::ofstream& file) override
   {
      file << experiment << " , ";
      file << elements << " , ";
      file << reader << " , ";
      file << writer << " , ";
      file << padding << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override
   {
      file << "Workload"
           << " , ";
      file << "Elements"
           << " , ";
      file << "reader"
           << " , ";
      file << "writer"
           << " , ";
      file << "padding"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};
// -------------------------------------------------------------------------------------



struct ALIGNMENT_workloadInfo : public nam::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t alignment;
   uint64_t padding;
   uint64_t timestamp = 0;

   ALIGNMENT_workloadInfo(std::string experiment, uint64_t alignment, uint64_t padding)
       : experiment(experiment), alignment(alignment), padding(padding) {}

   virtual std::vector<std::string> getRow() {
      return {
         experiment,
         std::to_string(alignment),
         std::to_string(padding),
         std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader() {
      return {"workload", "alignment", "padding", "timestamp"};
   }

   virtual void csv(std::ofstream& file) override {
      file << experiment << " , ";
      file << alignment << " , ";
      file << padding << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override {
      file << "Workload"
           << " , ";
      file << "Alignment"
           << " , ";
      file << "Padding"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};


struct ALIGNMENT_WS_workloadInfo : public nam::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t alignment;
   uint64_t workingSize;
   uint64_t timestamp = 0;

   ALIGNMENT_WS_workloadInfo(std::string experiment, uint64_t alignment, uint64_t workingSize)
       : experiment(experiment), alignment(alignment), workingSize(workingSize) {}

   virtual std::vector<std::string> getRow() {
      return {
         experiment,
         std::to_string(alignment),
         std::to_string(workingSize),
         std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader() {
      return {"workload", "alignment", "workingSize", "timestamp"};
   }

   virtual void csv(std::ofstream& file) override {
      file << experiment << " , ";
      file << alignment << " , ";
      file << workingSize << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override {
      file << "Workload"
           << " , ";
      file << "Alignment"
           << " , ";
      file << "WorkingSize"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};




template <typename F>
void readBlocks(uint64_t remote_addr,
                uint64_t*& buffer,
                uint64_t block_size,
                uint64_t number_blocks,
                uint64_t& blocks_read,
                uint64_t& inconsistencies,
                nam::rdma::RdmaContext& rctx,
                std::atomic<bool>& keep_running,
                F block_function) {
   using namespace nam;
   auto number_entries = block_size / sizeof(uint64_t);
   for (size_t b_i = 0; b_i < number_blocks; ++b_i) {
      if(!keep_running) break;
      threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
      blocks_read++;
      size_t addr = remote_addr + (block_size * b_i);
      // -------------------------------------------------------------------------------------
      auto start = utils::getTimePoint();
      // -------------------------------------------------------------------------------------
      block_function(number_entries,addr, buffer, rctx);
      // -------------------------------------------------------------------------------------
      auto end = utils::getTimePoint();
      threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
      // -------------------------------------------------------------------------------------
      uint64_t prev_version = buffer[0];
      for (size_t e_i = 0; e_i < number_entries; ++e_i) {
         if (prev_version < buffer[e_i]) {
            inconsistencies++;
         }
         prev_version = buffer[e_i];
      }
   }
}
}


void rdma_barrier_wait(uint64_t barrier_addr, uint64_t stage, uint64_t* local_barrier_buffer, nam::rdma::RdmaContext& rctx ) {
   {
      using namespace nam;
      nam::rdma::postFetchAdd(1, local_barrier_buffer, rctx, nam::rdma::completion::signaled, barrier_addr);
      int comp{0};
      ibv_wc wcReturn;
      while (comp == 0) {
         comp = nam::rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }
   }

   volatile auto* barrier_value = reinterpret_cast<uint64_t*>(local_barrier_buffer);
   uint64_t expected = (FLAGS_all_worker) * stage;

   while (*barrier_value != expected) {
      nam::rdma::postRead(const_cast<uint64_t*>(barrier_value), rctx, nam::rdma::completion::signaled, barrier_addr);
      int comp{0};
      ibv_wc wcReturn;
      while (comp == 0) {
         comp = nam::rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }

   }
}

void rdma_barrier_nam_wait(uint64_t barrier_addr, uint64_t stage, uint64_t* local_barrier_buffer, nam::rdma::RdmaContext& rctx ) {
   {
      using namespace nam;
      nam::rdma::postFetchAdd(1, local_barrier_buffer, rctx, nam::rdma::completion::signaled, barrier_addr);
      int comp{0};
      ibv_wc wcReturn;
      while (comp == 0) {
         comp = nam::rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }
   }

   volatile auto* barrier_value = reinterpret_cast<uint64_t*>(local_barrier_buffer);
   uint64_t expected = (FLAGS_worker * FLAGS_storage_nodes) * stage;

   while (*barrier_value != expected) {
      nam::rdma::postRead(const_cast<uint64_t*>(barrier_value), rctx, nam::rdma::completion::signaled, barrier_addr);
      int comp{0};
      ibv_wc wcReturn;
      while (comp == 0) {
         comp = nam::rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }
   }
}
