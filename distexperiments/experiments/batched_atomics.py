import config
from distexprunner import *

NUMBER_NODES = 5

parameter_grid = ParameterGrid(
    padding = [0],
    storageNodes = [1],
    worker=[1,2,4,8,16,32,64,128,256,512,1024,2048],
    batch = [1,4,16,64],
    locks=[2000000],
)


@reg_exp(servers=config.server_list[:NUMBER_NODES])
def compile(servers):
    servers.cd("/home/tziegler/rdma_synchronization/build/")
    cmake_cmd = f'cmake -D CMAKE_C_COMPILER=gcc-10 -D CMAKE_CXX_COMPILER=g++-10 -DCMAKE_BUILD_TYPE=Release ..'
    procs = [s.run_cmd(cmake_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))

    make_cmd = f'sudo make -j'
    procs = [s.run_cmd(make_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))
    

@reg_exp(servers=config.server_list[:NUMBER_NODES], params=parameter_grid, raise_on_rc=True, max_restarts=1)
def batch_benchmark(servers, padding,storageNodes, worker, batch,locks):    
    servers.cd("/home/tziegler/rdma_synchronization/build/frontend")        
    cmds = []
    cmd = f'numactl --membind=0 --cpunodebind=0 sudo ip netns exec ib0 ./batched_atomics -ownIp={servers[0].ibIp} -storage_node -worker={worker} -lock_count={locks} -padding={padding} -dramGB=10 -storage_nodes={storageNodes}'
    cmds += [servers[0].run_cmd(cmd)]

    work = worker
    numberNodes=1
    if worker >=4:
        work = int(worker/4)
        numberNodes=4
        
    for i in range(1, numberNodes+1):
        cmd = f'numactl --membind=0  sudo ip netns exec ib0 ./batched_atomics -ownIp={servers[i].ibIp} -worker={work} -all_worker={worker} -csvFile="batch_benchmark.csv" -run_for_seconds=30 -padding={padding} -tag={worker} -nopinThreads -lock_count={locks} -storage_nodes={storageNodes} -batch={batch}'
        cmds += [servers[i].run_cmd(cmd)]
        
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
