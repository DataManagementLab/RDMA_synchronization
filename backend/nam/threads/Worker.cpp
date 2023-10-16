#include "Worker.hpp"
// -------------------------------------------------------------------------------------
namespace nam {
namespace threads {
// -------------------------------------------------------------------------------------
thread_local Worker* Worker::tlsPtr = nullptr;
// -------------------------------------------------------------------------------------
Worker::Worker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId)
    : workerId(workerId),
      name(name),
      cpuCounters(name),
      cm(cm),
      nodeId_(nodeId),
      cctxs(FLAGS_storage_nodes),
      threadContext(std::make_unique<ThreadContext>()) {
   ThreadContext::tlsPtr = threadContext.get();
   // -------------------------------------------------------------------------------------
   // Connection to MessageHandler
   // -------------------------------------------------------------------------------------
   // First initiate connection
   for (uint64_t n_i = 0; n_i < FLAGS_storage_nodes; n_i++) {
      // -------------------------------------------------------------------------------------
      auto& ip = STORAGE_NODES[FLAGS_storage_nodes][n_i];
      cctxs[n_i].rctx = &(cm.initiateConnection(ip, rdma::Type::WORKER, workerId, nodeId));
      cctxs[n_i].wqe = 0;
      // -------------------------------------------------------------------------------------
   }

   // -------------------------------------------------------------------------------------
   // Second finish connection
   rdma::InitMessage* init = (rdma::InitMessage*)cm.getGlobalBuffer().allocate(sizeof(rdma::InitMessage)); 
   for (uint64_t n_i = 0; n_i < FLAGS_storage_nodes; n_i++) {
      init->nodeId = nodeId;
      init->threadId = workerId + (nodeId*FLAGS_worker);
      // -------------------------------------------------------------------------------------
      cm.exchangeInitialMesssage(*(cctxs[n_i].rctx), init);
      // -------------------------------------------------------------------------------------

      auto& msg = *reinterpret_cast<InitMessage*>((cctxs[n_i].rctx->applicationData));
      auto num_regions = msg.num_regions;
      std::cout << "num regions " << num_regions << "\n";
      // for(uint64_t t_i = 0; t_i < num_regions; t_i++){
      // hack only supports one region at the moment 
         catalog.insert({n_i,{.start = msg.mem_regions[0].offset, .size_bytes = msg.mem_regions[0].size_bytes, .region_id = (int)0}});
      // }
   }

   std::cout << "Connected" << std::endl;
}

// -------------------------------------------------------------------------------------
Worker::~Worker() {}
// -------------------------------------------------------------------------------------
}  // namespace threads
}  // namespace nam
