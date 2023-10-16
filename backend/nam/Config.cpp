// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DEFINE_double(dramGB, 1, "DRAM buffer pool size");
DEFINE_uint64(worker,1, "Number worker threads");
DEFINE_uint64(all_worker,1, "number of all worker threads in the cluster for barrier");
DEFINE_uint64(batchSize, 100, "batch size in free lists");
DEFINE_uint64(pageProviderThreads, 2, " Page Provider threads must be power two");
DEFINE_double(freePercentage, 1, "Percentage free for PP");
DEFINE_uint64(coolingPercentage, 10 , "Percentage cooling for PP");
DEFINE_double(evictCoolestEpochs, 0.1, "Percentage of coolest epchos choosen for eviction");
DEFINE_bool(csv, true , "If written to csv file or not");
DEFINE_string(csvFile, "stats.csv" , "filename for profiling output");
DEFINE_string(tag,"","descirption of experiment");
DEFINE_uint32(partitionBits, 6, "bits per partition");
DEFINE_uint32(page_pool_partitions, 8, "page pool partitions each is shifted by 512 byte to increase cache associativity");
// -------------------------------------------------------------------------------------
DEFINE_bool(backoff, true, "backoff enabled");
// -------------------------------------------------------------------------------------
DEFINE_bool(storage_node, false, "storage node");
DEFINE_uint64(storage_nodes, 1,"Number nodes participating");
DEFINE_double(rdmaMemoryFactor, 1.1, "Factor to be multiplied by dramGB"); // factor to be multiplied by dramGB
DEFINE_uint32(port, 7174, "port");
DEFINE_string(ownIp, "172.18.94.80", "own IP server");
// -------------------------------------------------------------------------------------
DEFINE_uint64(pollingInterval, 16, " Number of unsignaled messages before a signaled (power of 2)");
DEFINE_bool(read, true, "read protocol");
DEFINE_bool(random, false, "use random pages");
DEFINE_uint64(messageHandlerThreads, 4, " number message handler ");
DEFINE_uint64(messageHandlerMaxRetries, 10, "Number retries before message gets restarted at client"); // prevents deadlocks but also mitigates early aborts
// -------------------------------------------------------------------------------------
DEFINE_uint32(sockets, 2 , "Number Sockets");
DEFINE_uint32(socket, 0, " Socket we are running on");
DEFINE_bool(pinThreads, true, " Pin threads");
DEFINE_bool(cpuCounters,true, " CPU counters profiling ");
