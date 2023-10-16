#include "WorkerCounters.hpp"
#include "../CounterRegistry.hpp"
// -------------------------------------------------------------------------------------
namespace nam {
namespace profiling {
// -------------------------------------------------------------------------------------
WorkerCounters::WorkerCounters(){
   CounterRegistry::getInstance().registerWorkerCounter(this);
}
// -------------------------------------------------------------------------------------
WorkerCounters::~WorkerCounters(){
   CounterRegistry::getInstance().deregisterWorkerCounter(this);
}
// -------------------------------------------------------------------------------------
}  // profiling
}  // nam
