import config
from distexprunner import *

NUMBER_NODES = 2

parameter_grid = ParameterGrid(
    numberNodes=[2],
    options=["","-unlock_write", "-speculative_read","-speculative_read -write_combining", "-speculative_read -write_combining -order_release", "-nowrite", "-nowrite -speculative_read", "-nowrite -speculative_read -order_release"],
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
def locking_ablation(servers, numberNodes, options):    
    servers.cd("/home/tziegler/rdma_synchronization/build/frontend")

    cmds = []
    
    cmd = f'numactl --membind=0 --cpunodebind=0 ./lock -ownIp={servers[0].ibIp} -storage_node '
    cmds += [servers[0].run_cmd(cmd)]
    
    cmd = f'numactl --membind=0 --cpunodebind=0 ./lock -ownIp={servers[1].ibIp}  -csvFile="locking_ablation.csv" -run_for_seconds=10 {options}'
    cmds += [servers[1].run_cmd(cmd)]
        
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART

