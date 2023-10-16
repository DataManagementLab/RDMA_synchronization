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
// avoids destruction of objects before remote side finished
// -------------------------------------------------------------------------------------
class NAM
{

  public:
   //! Default constructor
   NAM();

   //! Destructor
   ~NAM();
   // -------------------------------------------------------------------------------------
   // Deleted constructors
   //! Copy constructor
   NAM(const NAM& other) = delete;
   //! Move constructor
   NAM(NAM&& other) noexcept = delete;
   //! Copy assignment operator
   NAM& operator=(const NAM& other) = delete;
   //! Move assignment operator
   NAM& operator=(NAM&& other) noexcept = delete;
   // -------------------------------------------------------------------------------------
   rdma::CM<rdma::InitMessage>& getCM() { return *cm; }
   // -------------------------------------------------------------------------------------
   NodeID getNodeID() { return nodeId; }
   // -------------------------------------------------------------------------------------
   void startProfiler(profiling::WorkloadInfo& wlInfo) {
      pt.running = true;
      profilingThread.emplace_back(&profiling::ProfilingThread::profile, &pt, nodeId, std::ref(wlInfo));
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
      
   // -------------------------------------------------------------------------------------
   void startAndConnect() {
      std::thread connectionThread([&]() {
         using namespace rdma;
         rdma::InitMessage* initServer = (rdma::InitMessage*)cm->getGlobalBuffer().allocate(sizeof(rdma::InitMessage));
         // -------------------------------------------------------------------------------------
         size_t numConnections = (FLAGS_worker * FLAGS_storage_nodes);
         std::cout << "Waiting for connections " << numConnections << "\n";
         while (cm->getNumberIncomingConnections() != (numConnections))
            ;  // block until client is connected

         std::vector<RdmaContext*> rdmaCtxs(cm->getIncomingConnections());  // get cm ids of incomming

         for (auto* rContext : rdmaCtxs) {
            // -------------------------------------------------------------------------------------
            if (rContext->type != Type::WORKER) { throw; }
            // -------------------------------------------------------------------------------------
            initServer->nodeId = nodeId; 
            initServer->threadId = 1000;
            initServer->num_regions = catalog.size();
            ensure(initServer->num_regions < MAX_REGIONS);
            for (auto& it : catalog) {
               // Do stuff
               initServer->mem_regions[it.second.region_id].offset = (uintptr_t)it.second.start;
               initServer->mem_regions[it.second.region_id].size_bytes = (uintptr_t)it.second.size_bytes;
            }

            // -------------------------------------------------------------------------------------
            cm->exchangeInitialMesssage(*(rContext), initServer);
            
         }
         std::cout << "Finished connection " << "\n";

      });
      startWorkerPool();
      connectionThread.join();
   };

   void startAndConnect(std::function<void()> startup) {
      std::thread connectionThread([&]() {
         using namespace rdma;
         rdma::InitMessage* initServer = (rdma::InitMessage*)cm->getGlobalBuffer().allocate(sizeof(rdma::InitMessage));
         // -------------------------------------------------------------------------------------
         size_t numConnections = (FLAGS_worker);
         std::cout << "Waiting for connections " << numConnections << "\n";
         while (cm->getNumberIncomingConnections() != (numConnections))
            ;  // block until client is connected

         std::vector<RdmaContext*> rdmaCtxs(cm->getIncomingConnections());  // get cm ids of incomming

         for (auto* rContext : rdmaCtxs) {
            // -------------------------------------------------------------------------------------
            if (rContext->type != Type::WORKER) { throw; }
            // -------------------------------------------------------------------------------------
            initServer->nodeId = nodeId; 
            initServer->threadId = 1000;
            initServer->num_regions = catalog.size();
            ensure(initServer->num_regions < MAX_REGIONS);
            for (auto& it : catalog) {
               // Do stuff
               initServer->mem_regions[it.second.region_id].offset = (uintptr_t)it.second.start;
               initServer->mem_regions[it.second.region_id].size_bytes = (uintptr_t)it.second.size_bytes;
            }

            // -------------------------------------------------------------------------------------
            cm->exchangeInitialMesssage(*(rContext), initServer);
            
         }
      });
      startup();
      connectionThread.join();
   };

   void startWorkerPool(){
         workerPool = std::make_unique<threads::WorkerPool>(*cm, nodeId);
   }

   threads::WorkerPool& getWorkerPool(){
      return *workerPool;
   }
   void registerMemoryRegion(std::string name, size_t bytes){
      if (catalog.count(name)) throw;
      uintptr_t buffer = (uintptr_t)static_cast<void*>(cm->getGlobalBuffer().allocate(bytes,64));
      MemoryRegionDesc desc;
      desc.start = buffer;
      desc.size_bytes = bytes;
      desc.region_id = regions++;
      catalog[name] = desc;
   }

   MemoryRegionDesc& getMemoryRegion(std::string name){
      return catalog[name];
   }

  private:
   NodeID nodeId = 0;
   std::unique_ptr<rdma::CM<rdma::InitMessage>> cm;
   std::unique_ptr<profiling::RDMACounters> rdmaCounters;
   profiling::ProfilingThread pt;
   std::vector<std::thread> profilingThread;
   std::unordered_map<std::string,MemoryRegionDesc> catalog; // ptr, size of region
   int regions =0;
   std::unique_ptr<threads::WorkerPool> workerPool;

};
// -------------------------------------------------------------------------------------
}  // namespace scalestore
