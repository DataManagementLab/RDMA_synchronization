import config
from distexprunner import *

NUMBER_NODES = 2

parameter_grid = ParameterGrid(
    numberNodes=[2],
    write_speed = [1,4,16,32],
    read_size = [128,512,2048,8192,32768],
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
def broken_reads(servers, numberNodes, write_speed, read_size ):    
    servers.cd("/home/tziegler/rdma_synchronization/build/frontend")

    cmds = []
    
    cmd = f'numactl --membind=0 --cpunodebind=0 ./broken_remote_write -ownIp={servers[0].ibIp} -storage_node -worker=2'
    cmds += [servers[0].run_cmd(cmd)]
    
    cmd = f'numactl --membind=0 --cpunodebind=0 ./broken_remote_write -ownIp={servers[1].ibIp} -worker=2 -csvFile="broken_ordering_rwrite.csv" -run_for_seconds=300 -block_size={read_size} -write_speed={write_speed}'
    cmds += [servers[1].run_cmd(cmd)]
        
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
    
