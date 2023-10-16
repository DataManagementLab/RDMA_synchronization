#pragma once
#include "Defs.hpp"
#include "ThreadContext.hpp"
#include "nam/profiling/counters/CPUCounters.hpp"
#include "nam/profiling/counters/WorkerCounters.hpp"
#include "nam/rdma/CommunicationManager.hpp"
// -------------------------------------------------------------------------------------
namespace nam {
namespace threads {
using namespace rdma;
constexpr static bool OPTIMIZED_COMPLETION = true;
// -------------------------------------------------------------------------------------
struct Worker{
   // -------------------------------------------------------------------------------------
   static thread_local Worker* tlsPtr;
   static inline Worker& my() { return *Worker::tlsPtr; }
   // -------------------------------------------------------------------------------------
   uint64_t workerId;
   std::string name;
   // -------------------------------------------------------------------------------------
   profiling::CPUCounters cpuCounters;
   // -------------------------------------------------------------------------------------
   profiling::WorkerCounters counters;
   // -------------------------------------------------------------------------------------
   // RDMA
   // -------------------------------------------------------------------------------------
   // context for every connection
   struct ConnectionContext {
      rdma::RdmaContext* rctx;
      uint64_t wqe;  // wqe currently outstanding
   };
   // -------------------------------------------------------------------------------------
   struct PartitionInfo {
      uintptr_t offset;
      uint64_t begin;
      uint64_t end;
      NodeID nodeId;
   };

   // -------------------------------------------------------------------------------------
   rdma::CM<rdma::InitMessage>& cm;
   NodeID nodeId_;
   std::vector<ConnectionContext> cctxs;
   std::unique_ptr<ThreadContext> threadContext;
   std::unordered_map<int,MemoryRegionDesc> catalog; // ptr, size of region
   Worker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId);
   ~Worker();
};
// -------------------------------------------------------------------------------------
}  // namespace threads
}  // namespace nam
