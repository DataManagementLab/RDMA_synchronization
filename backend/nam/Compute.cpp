#include "Compute.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>

namespace nam {
Compute::Compute() {
   cm = std::make_unique<rdma::CM<rdma::InitMessage>>();
   rdmaCounters = std::make_unique<profiling::RDMACounters>();
   workerPool = std::make_unique<threads::WorkerPool>(*cm, 0);
}

Compute::~Compute() {
   workerPool.reset();
}
}  // namespace nam
