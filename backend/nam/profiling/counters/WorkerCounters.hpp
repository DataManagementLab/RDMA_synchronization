#pragma once
// -------------------------------------------------------------------------------------
#include "Defs.hpp"
#include "nam/utils/Time.hpp"
// -------------------------------------------------------------------------------------
#include <array>
#include <atomic>
#include <string_view>

// -------------------------------------------------------------------------------------

/*
// #define INC_WORKER_COUNTER(COUNTER)                                                                     \
//    {                                                                                                    \
//       auto local = profiling::WorkerCounter::getCounter().counters[COUNTER].load();                     \
//       local++;                                                                                          \
//       profiling::WorkerCounter::getCounter().counters[COUNTER].store(local, std::memory_order_relaxed); \
//    }


// #define INC_WORKER_COUNTER_BY(COUNTER, value)                                    \
//    {                                                                                                    \
//       auto local = profiling::WorkerCounter::getCounter().counters[COUNTER].load();                     \
//       local+= value;                                                                                    \
//       profiling::WorkerCounter::getCounter().counters[COUNTER].store(local, std::memory_order_relaxed); \
//    }


// #define ASSIGN_WORKER_COUNTER(COUNTER, value)                                                           \
//    {                                                                                                    \
//       profiling::WorkerCounter::getCounter().counters[COUNTER].store(value, std::memory_order_relaxed); \
//    }

*/
namespace nam
{
namespace profiling
{
struct WorkerCounters {
   // -------------------------------------------------------------------------------------
   enum Name {
      tx_p,
      abort,
      latency,
      ssd_pages_read,
      ssd_pages_written,
      rdma_pages_tx,
      rdma_pages_rx,
      mh_msgs_handled,
      pp_rounds,
      pp_rdma_evicted,
      pp_rdma_received,
      pp_latency_p1_incoming_requests,
      pp_latency_p2_cooling,
      pp_latency_p3_select,
      pp_latency_p4_send_requests,
      pp_latency_p5_incoming_responses,
      pp_latency_p6_send_responses,
      COUNT,
   };
   // -------------------------------------------------------------------------------------
   static const constexpr inline std::array<std::string_view, COUNT> workerCounterTranslation{
       "tx/sec",
       "abort",
       "latency",
       "pages r (SSD)",
       "pages w (SSD)",
       "pages tx (RDMA)",
       "pages rx (RDMA)",
       "msg handled",
       "pp_rounds",
       "pp_rdma_evicted",
       "pp_rdma_received",
       "pp_latency_p1_incoming_requests",
       "pp_latency_p2_cooling",
       "pp_latency_p3_select",
       "pp_latency_p4_send_requests",
       "pp_latency_p5_incoming_responses",
       "pp_latency_p6_send_responses",
   };
   static_assert(workerCounterTranslation.size() == COUNT);
   // -------------------------------------------------------------------------------------
   struct LOG_ENTRY{
      const std::string_view name;
      const LOG_LEVEL level;
   };

   static const constexpr inline std::array<LOG_ENTRY, COUNT> workerCounterLogLevel{{
       {"tx/sec", LOG_LEVEL::RELEASE},
       {"abort", LOG_LEVEL::RELEASE},
       {"latency", LOG_LEVEL::RELEASE},
       {"ssd_pages_read", LOG_LEVEL::RELEASE},
       {"ssd_pages_written", LOG_LEVEL::RELEASE},
       {"rdma_pages_tx", LOG_LEVEL::CSV},
       {"rdma_pages_rx", LOG_LEVEL::CSV},
       {"mh_msgs_handled", LOG_LEVEL::RELEASE},
       {"pp_rounds", LOG_LEVEL::CSV},
       {"pp_rdma_evicted", LOG_LEVEL::CSV},
       {"pp_rdma_received", LOG_LEVEL::CSV},
       {"pp_latency_p1_incoming_requests", LOG_LEVEL::CSV},
       {"pp_latency_p2_cooling", LOG_LEVEL::CSV},
       {"pp_latency_p3_select", LOG_LEVEL::CSV},
       {"pp_latency_p4_send_requests", LOG_LEVEL::CSV},
       {"pp_latency_p5_incoming_responses", LOG_LEVEL::CSV},
       {"pp_latency_p6_send_responses", LOG_LEVEL::CSV},
   }};
   // -------------------------------------------------------------------------------------
   
   WorkerCounters();
   ~WorkerCounters();
   // -------------------------------------------------------------------------------------

   __attribute__((always_inline)) void incr(const Name& name)
   {
      if( workerCounterLogLevel[name].level > ACTIVE_LOG_LEVEL)
         return;
      
      auto local = counters[name].load();
      local++;
      counters[name].store(local, std::memory_order_relaxed);
   }



   __attribute__((always_inline)) uint64_t getTimePoint_for(const Name& name){
        if( workerCounterLogLevel[name].level > ACTIVE_LOG_LEVEL)
           return 0;
        
        return utils::getTimePoint();
   }

   
   __attribute__((always_inline)) void incr_by(const Name& name, uint64_t increment)
   {
      if ( workerCounterLogLevel[name].level > ACTIVE_LOG_LEVEL)
         return;
            
      auto local = counters[name].load();
      local+= increment;
      counters[name].store(local, std::memory_order_relaxed);
   }
   
   std::atomic<uint64_t> counters[COUNT] = {0};
};

}  // namespace profiling
}  // namespace nam
