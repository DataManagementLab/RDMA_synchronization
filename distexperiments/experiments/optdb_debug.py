import config
from distexprunner import *

NUMBER_NODES = 4

parameter_grid = ParameterGrid(
    padding = [0],
    worker=["104 -nopinThreads"],
    locks=[200],
    options=["-farm"],
    # options=["-versioning" , "-CRC", "farm"],
    footer=["-footer", "-nofooter"],
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
def nam_benchmark(servers, padding, worker,locks, options, footer):    
    servers.cd("/home/tziegler/rdma_synchronization/build/frontend")        
    cmds = []
    # if (options != "") and (padding == 0):
    #     return
    tag = "OPTDB"
    
    for i in range(0, NUMBER_NODES):
        cmd = f'numactl --membind=0 sudo ip netns exec ib0  ./optdb_experiment -ownIp={servers[i].ibIp} -worker={worker} -csvFile="optdb_debug.csv" -run_for_seconds=300 -padding={padding} -tag={tag} -lock_count={locks} -storage_nodes={NUMBER_NODES} -storage_node {options} {footer} -dramGB=10'
        cmds += [servers[i].run_cmd(cmd)]
        sleep(1)
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
