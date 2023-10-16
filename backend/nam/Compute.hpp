#pragma once
// -------------------------------------------------------------------------------------
#include "profiling/ProfilingThread.hpp"
#include "profiling/counters/RDMACounters.hpp"
#include "rdma/CommunicationManager.hpp"
#include "threads/CoreManager.hpp"
#include "threads/WorkerPool.hpp"
#include "nam/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <memory>

namespace nam
{
// -------------------------------------------------------------------------------------
class Compute
{

  public:
   //! Default constructor
   Compute();
   //! Destructor
   ~Compute();
   // -------------------------------------------------------------------------------------
   // Deleted constructors
   //! Copy constructor
   Compute(const Compute& other) = delete;
   //! Move constructor
   Compute(Compute&& other) noexcept = delete;
   //! Copy assignment operator
   Compute& operator=(const Compute& other) = delete;
   //! Move assignment operator
   Compute& operator=(Compute&& other) noexcept = delete;
   // -------------------------------------------------------------------------------------
   rdma::CM<rdma::InitMessage>& getCM() { return *cm; }
   // -------------------------------------------------------------------------------------
   threads::WorkerPool& getWorkerPool(){
      return *workerPool;
   }
   // -------------------------------------------------------------------------------------
   void startProfiler(profiling::WorkloadInfo& wlInfo) {
      pt.running = true;
      profilingThread.emplace_back(&profiling::ProfilingThread::profile, &pt, 0, std::ref(wlInfo));
   }
   // -------------------------------------------------------------------------------------
   void stopProfiler()
   {
      if (pt.running == true) {
         pt.running = false;
         for (auto& p : profilingThread)
            p.join();
         profilingThread.clear();
      }
      std::locale::global(std::locale("C")); // hack to restore locale which is messed up in tabulate package
   };
      
  private:
   std::unique_ptr<rdma::CM<rdma::InitMessage>> cm;
   std::unique_ptr<profiling::RDMACounters> rdmaCounters;
   profiling::ProfilingThread pt;
   std::vector<std::thread> profilingThread;
   std::unique_ptr<threads::WorkerPool> workerPool;
};
// -------------------------------------------------------------------------------------
}  // namespace scalestore
