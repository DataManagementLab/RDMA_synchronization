// -------------------------------------------------------------------------------------
#include "counters/WorkerCounters.hpp"
#include "counters/CPUCounters.hpp"
#include "counters/RDMACounters.hpp"
// -------------------------------------------------------------------------------------
#include <mutex>
#include <iostream>
#include <vector>
#include <algorithm>
#include <atomic>
#include <unordered_map>
#include <string>
#include <cmath>
// -------------------------------------------------------------------------------------
namespace nam {
namespace profiling {

struct CounterRegistry{

   // -------------------------------------------------------------------------------------
   static CounterRegistry& getInstance(){
      static CounterRegistry instance;
      return instance;
   }
   // -------------------------------------------------------------------------------------
   void aggregateWorkerCounters(std::vector<uint64_t>& counters){
      std::unique_lock<std::mutex> guard(workerMutex);
      for (auto* c_ptr : workerCounters) {
         for (uint64_t c_i = 0; c_i < WorkerCounters::COUNT; c_i++)
            counters[c_i] += c_ptr->counters[c_i].exchange(0);
      }
   };
   
   void registerWorkerCounter(WorkerCounters* counter){
      std::unique_lock<std::mutex> guard(workerMutex);
      workerCounters.push_back(counter);
   }

   
   void deregisterWorkerCounter(WorkerCounters* counter){
      std::unique_lock<std::mutex> guard(workerMutex);
      workerCounters.erase(std::remove_if(begin(workerCounters), end(workerCounters), [&](WorkerCounters* c) { return (c == counter); }),
                           end(workerCounters));
   }
   // -------------------------------------------------------------------------------------

   void aggregateCPUCounter(std::unordered_map<std::string, double>& counters)
   {
      std::unique_lock<std::mutex> guard(cpuMutex);
      for (auto* c_ptr : cpuCounters) {
         if (c_ptr->threadName.rfind("worker", 0) == 0)
         {
            c_ptr->e->stopCounters();
            auto eMap = c_ptr->e->getCountersMap();
            for(auto& [eName,eValue] : eMap){ 
               if(std::isnan(eValue)){
                  counters[eName] += 0;
                  continue;
               }
               counters[eName] += eValue;
            }
            c_ptr->e->startCounters();
         }
      }
   }


   void registerCPUCounter(CPUCounters* counter){
      std::unique_lock<std::mutex> guard(cpuMutex);
      cpuCounters.push_back(counter);
   }

   void deregisterCPUCounter(CPUCounters* counter){
      std::unique_lock<std::mutex> guard(cpuMutex);
      cpuCounters.erase(std::remove_if(begin(cpuCounters), end(cpuCounters), [&](CPUCounters* c) { return (c == counter); }),
                           end(cpuCounters));
   }

   // -------------------------------------------------------------------------------------
   void aggregateRDMACounters(std::vector<double>& counters){
      std::unique_lock<std::mutex> guard(rdmaMutex);
      for (auto* c_ptr : rdmaCounters) {
         counters[RDMACounters::sentGB] = c_ptr->getSentGB(); 
         counters[RDMACounters::recvGB] = c_ptr->getRecvGB();
      }
   };
   
   void registerRDMACounter(RDMACounters* counter){
      std::unique_lock<std::mutex> guard(rdmaMutex);
      rdmaCounters.push_back(counter);
   }

   void deregisterRDMACounter(RDMACounters* counter){
      std::unique_lock<std::mutex> guard(rdmaMutex);
      rdmaCounters.erase(std::remove_if(begin(rdmaCounters), end(rdmaCounters), [&](RDMACounters* c) { return (c == counter); }),
                           end(rdmaCounters));
   }

   
   // -------------------------------------------------------------------------------------
   std::mutex workerMutex;
   std::vector<WorkerCounters*> workerCounters;
   // -------------------------------------------------------------------------------------
   std::mutex cpuMutex; 
   uint64_t cpuCounterId;
   std::vector<CPUCounters*> cpuCounters;
   // -------------------------------------------------------------------------------------
   std::mutex rdmaMutex; 
   std::vector<RDMACounters*> rdmaCounters;
};


}  // profiling
}  // nam
