import experiment
import time
import config


class exp1(experiment.Base):
    SERVERS = [
        experiment.Server('node1', '127.0.0.1', custom_field=42)
    ]
    def experiment(self, target):
        long_cmd = target('node1').run_cmd('sleep 4', stdout=experiment.Printer(), stderr=experiment.Printer())

        target('node1').run_cmd('ls', stdout=[experiment.Printer(), experiment.Logfile('ls.log', append=True)])
        # self.SERVERS[0].data.custom_field = 69
        print(target('node1').data.custom_field)

        printer = experiment.Printer(fmt='stdin="{line}"\n', rstrip=True)
        cmd = target('node1').run_cmd('bash -c \'read p && echo $p\'', stdout=printer)
        # cmd.kill()
        cmd.stdin('foobar\n')

        long_cmd.wait()     # We need to wait, else all running commands are killed


class exp2(experiment.Base):
    SERVERS = exp1.SERVERS + [
        experiment.Server('node2', '127.0.0.1', config.SERVER_PORT+1)
    ]
    def experiment(self, target):
        procs = []
        for i, s in enumerate(self.SERVERS):
            try:
                p = target(s).run_cmd(f'sleep {5*(i+1)}', stdout=experiment.Printer(), stderr=experiment.Printer())
            except experiment.errors.NoConnectionError:
                continue
            procs.append(p)

        rcs = [proc.wait() for proc in procs]
        assert(all(rc == 0 for rc in rcs))



def exp3_factory(a, b):
    class exp3(experiment.Base):
        SERVERS = [
            experiment.Server('node', '127.0.0.1')
        ]
        def experiment(self, target):
            cmd = f'./foobar -a {a} -b {b}'
            print(cmd)
    return exp3

a = ['x', 'y']
b = range(5, 10)
experiment.factory.Grid(exp3_factory, a, b)


class exp4(experiment.Base):
    SERVERS = [
        experiment.Server('node1', '127.0.0.1')
    ]
    def experiment(self, target):
        env = {
            'OMP_NUM_THREADS': 8
        }
        target('node1').run_cmd('env', stdout=experiment.Printer(), stderr=experiment.Printer(), env=env).wait()



# class AA_CompileJob(experiment.Base):
#     SERVERS = exp1.SERVERS
#     RUN_ALWAYS = True       # immune to filters
#     def experiment(self, target):
#         cmake_cmd = 'cmake -B../build -S../'
#         procs = []
#         for s in self.SERVERS:
#             printer = experiment.Printer(fmt=f'{s.id}: '+'{line}')
#             try:
#                 p = target(s).run_cmd(cmake_cmd, stdout=printer, stderr=printer)
#             except experiment.errors.NoConnectionError:
#                 continue
#             procs.append(p)
#         [proc.wait() for proc in procs]

#         make_cmd = 'make -j -C ../build'
#         procs = []
#         for s in self.SERVERS:
#             printer = experiment.Printer(fmt=f'{s.id}: '+'{line}')
#             try:
#                 p = target(s).run_cmd(make_cmd, stdout=printer, stderr=printer)
#             except experiment.errors.NoConnectionError:
#                 continue
#             procs.append(p)
#         rcs = [proc.wait() for proc in procs]
#         assert(all(rc == 0 for rc in rcs))


MAX_RESTARTS = 3
class restart(experiment.Base):
    SERVERS = [
        experiment.Server('node1', '127.0.0.1', custom_field=42)
    ]
    def experiment(self, target):
        cmd = target('node1').run_cmd('bash -c "sleep 1 && exit -11"', stdout=experiment.Printer())
        rcs = [cmd.wait()]

        global MAX_RESTARTS
        MAX_RESTARTS -= 1
        if MAX_RESTARTS == 0:
            return

        if not all(rc == 0 for rc in rcs):
            raise experiment.actions.Restart


class async_restart(experiment.Base):
    SERVERS = [
        experiment.Server('node1', '127.0.0.1', custom_field=42)
    ]
    def experiment(self, target):
        procs = []

        proc = target('node1').run_cmd('bash -c "sleep 1 && exit 0"', stdout=experiment.Printer())
        procs.append(proc)

        while True:
            rcs = [proc.wait(block=False) for proc in procs]
            if any(rc is not None and rc != 0 for rc in rcs):
                raise experiment.actions.Restart

            if all(rc == 0 for rc in rcs):
                break

            time.sleep(0.1)