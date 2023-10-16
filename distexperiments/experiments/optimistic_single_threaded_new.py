import config
from distexprunner import *

NUMBER_NODES = 2

parameter_grid = ParameterGrid(
    numberNodes=[2],
    options=["-CRC", "-farm" , "-versioning", "-broken", "-pessimistic"],
    footer=["-nofooter","-footer"],
    blocks=[64, 256, 1024, 4096, 16384, 65536],
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
def locking_ablation(servers, numberNodes, options, footer, blocks):    
    servers.cd("/home/tziegler/rdma_synchronization/build/frontend")
    if (options == "-pessimistic") and (options == "-footer"):
        return
    
    cmds = []
    
    cmd = f'numactl --membind=0 sudo ip netns exec ib0 ./optmistic_benchmark -ownIp={servers[0].ibIp} -storage_node -block_size={blocks}'
    cmds += [servers[0].run_cmd(cmd)]
    
    cmd = f'numactl --membind=0 sudo ip netns exec ib0 ./optmistic_benchmark -ownIp={servers[1].ibIp}  -csvFile="optimistic_runner_st_new.csv" -run_for_seconds=30 {options} -block_size={blocks} {footer} -readratios=100'
    cmds += [servers[1].run_cmd(cmd)]
        
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
