import config
from distexprunner import *

NUMBER_NODES = 2

parameter_grid = ParameterGrid(
    numberNodes=[2],
    alignment = [8,16,32,64,128,256,512,1024,2048,4096,8192,16384,32768,65536],
    padding = [0,8],
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
def alignment(servers, numberNodes, alignment, padding):    
    servers.cd("/home/tziegler/rdma_synchronization/build/frontend")

    cmds = []
    
    cmd = f'numactl --membind=0 --cpunodebind=0 ./atomic_alignment -ownIp={servers[0].ibIp} -storage_node -worker=36'
    cmds += [servers[0].run_cmd(cmd)]
    
    cmd = f'numactl --membind=0 --cpunodebind=0 ./atomic_alignment -ownIp={servers[1].ibIp} -worker=36 -csvFile="alignment.csv" -run_for_seconds=10 -alignment={alignment} -padding={padding} -record_latency'
    cmds += [servers[1].run_cmd(cmd)]
        
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
    
