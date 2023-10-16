# distexprunner

A suite to write and run distributed experiments across multiple network nodes.

* *Note: Old experiment syntax can still be read via `--compatibility-mode`*
* *Note: Version 1 with `experiment.Base` is deprecated for writing new experiments.*


## Demo

![](client_demo.gif)


## Installation

The best way to integrate distexprunner in a project is to add it as a submodule:

```
mkdir distexperiments/ && cd distexperiments/
git submodule add https://github.com/mjasny/distexprunner
cp -r distexprunner/examples .
```

*Note: For readability, all commands only list the required executeable instead of the full path. In this current setup the server command would correspond to: `python3 distexprunner/{server,client}.py`*.

At this stage you can already try the functionality by running locally the following commands in different shells. (e.g. using `tmux`)

Start one server instance. If you need multiple on the same machine you need to specify a different port with `--port`. In general only one instance is needed because it is capable to run multiple commands and even experiments in parallel without interfering with eachother.

`python -vv server.py`

Now a client is ready to connect to the servers and execute experiments.

`python -vv client.py examples/`

The folder parameter (`examples/`) is the search path for new experiments. It scans recursively all `.py` files for experiments which are registered with `@reg_exp(...)`. You can add multiple folders and also refer directly to a `.py` file if you want to run a subset of all experiments.

The order of the arguments is used as an execution order. This might be useful to do compilation jobs beforehand: `python client.py -vv compile.py scaleout.py`.



## Writing experiments

```python
import config
from distexprunner import *


server_list = ServerList(
    Server('node01', '127.0.0.1', config.SERVER_PORT, optional_field=True),
)

@reg_exp(servers=server_list)
def ls(servers):
    servers[0].run_cmd('ls').wait()

@reg_exp(servers=server_list)
def kill_yes(servers):
    for s in servers[lambda s: s.optional_field==True]:
        yes_cmd = s.run_cmd('yes > /dev/null')
        sleep(3)    # not time.sleep()!
        yes_cmd.kill()
```

### Experiment registration

Experiments are functions decorated with `@reg_exp(...)` and are grouped in `.py`. The order in which they appear in the file is the same in which they are executed. The function name is used as the experiment name, for parameter grids a suffix is added.

The decorator takes the following arguments:
- **servers** => ServerList *(required)*
- **params** => ParameterGrid *(optional)*
- **max_restarts** => int *(optional, default unlimited=0)*

The `ServerList` needs to contain all `Servers` which are needed for the experiment. Upon execution it is supplied to the function, servers can be selected via the `[]` operator using an int-index, the server id or a lambda filter predicate.


### ServerList definition

A `Server` has two mandatory construction arguments: 
- **id** => str *(required)*
- **ip** => str *(required, can also be a hostname)*
- **port** => int *(optional, default 20000)*
- **\*\*kwargs** => dict *(optional, additional attributes)

Before an experiment is run a connection to all `Server` in the `ServerList` is made and at the end the connection is terminated, which kills all still running on the Server. It is recommended to use a `config.py` for configuration parameters shared across different experiment files.

### Command execution

Inside the experiment function commands can be executed on servers using: `cmd = server.run_cmd(...)`. 

It takes the following arguments:
- **cmd** => str *(required)*
- **stdout** => Console/File *(optional, can be a single object or list which is called for each line)*
- **stderr** => Console/File *(optional, can be a single object or list which is called for each line)*
- **env** => dict *(optional, adds environment variables)*

#### Command actions

It returns on object `cmd = run_cmd(...)` with the following callable methods:

- **cmd.wait(block=True)** => int

Is a by default blocking call which waits until the spawned process on the server finishes and returns the returncode. If `block=False` it can return `None` if the process is still running. If the process already finished the returncode is returned immediately. 
- **cmd.kill()** => int

Kills the running process and returns a returncode.

- **cmd.stdin(close=False)** => None

Feeds a string into stdin of the running command. `\n` is needed at the end to simulate an ENTER keypress. If `close=True` then the stdin to the process is closed.

### Experiment behaviour

If the `experiment()` function returns before running commands are terminated they are killed. So it is advised to use `.wait()` calls on running commands.

The experiment function can return `Action.RESTART` in case some returncodes of previous commands are `!=0` to restart the current experiment indefinetly or `max_restarts` times.


### Experiment Factory

`ParameterGrid` can be used to do parameterized grid executions. The experiment is called for the kartesian product (using `itertools.product`) of all parameters (e.g. `a` and `b`). These named arguments are then given to the experiment function upon runtime. The parameters are also used to add a unique suffix the the experiment name, e.g.: `grid__a=4_b=4_to_file=False`.

```python
import config
from distexprunner import *


server_list = ServerList(
    Server('node01', '127.0.0.1', config.SERVER_PORT, optional_field=True),
)

parameter_grid = ParameterGrid(
    a=range(1, 5),
    b=[2, 4],
    to_file=[True, False]
)

@reg_exp(servers=server_list, params=parameter_grid)
def grid(servers, a, b, to_file):
    for s in servers:
        stdout = File('grid.log', append=True)
        if not to_file:
            stdout = [stdout, Console(fmt=f'{s.id}: %s')]

        s.run_cmd(f'echo {a} {b}', stdout=stdout).wait()


```

This generates the following set of experiments:

```
experiments = [
    grid__a=1_b=2_to_file=True, grid__a=1_b=2_to_file=False, 
    grid__a=1_b=4_to_file=True, grid__a=1_b=4_to_file=False, 
    grid__a=2_b=2_to_file=True, grid__a=2_b=2_to_file=False, 
    grid__a=2_b=4_to_file=True, grid__a=2_b=4_to_file=False, 
    grid__a=3_b=2_to_file=True, grid__a=3_b=2_to_file=False, 
    grid__a=3_b=4_to_file=True, grid__a=3_b=4_to_file=False, 
    grid__a=4_b=2_to_file=True, grid__a=4_b=2_to_file=False, 
    grid__a=4_b=4_to_file=True, grid__a=4_b=4_to_file=False 
]
```


## Usage

Examples can be found in [examples/](./examples/).


## Client

```
usage: client.py [-h] [-v] [--resume] [--compatibility-mode] [--slack-webhook SLACK_WEBHOOK] experiment [experiment ...]

Distributed Experiment Runner Client Instance

positional arguments:
  experiment            path to experiments, folders are searched recursively, order is important

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         -v WARN -vv INFO -vvv DEBUG
  --resume              Resume execution of experiments from last run
  --compatibility-mode  Activate compatibiliy mode for class x(experiment.Base)
  --slack-webhook SLACK_WEBHOOK
                        Notify to slack when execution finishes
  --progress            Display progressbar, but disables logging
  --log LOG             Log into file
```

- `experiment` Used to search for experiments
- `--resume` In case of interruption, only runs experiments which are not present in file `.distexprunner`
- `--slack-webhook` if supplied a notification is sent to the channel after all experiments are run (see: https://api.slack.com/tutorials/slack-apps-hello-world)
- `--progress` Displays a progressbar, but needs to completely disable logging output. Therefore use in conjuncition with `--log`
- `--log` Appends all logging output in addition to stderr into a file.


## Server

```
usage: server.py [-h] [-v] [-ip IP] [-p PORT] [-rf] [-mi MAX_IDLE]

Distributed Experiment Runner Server

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         -v WARN -vv INFO -vvv DEBUG
  -ip IP, --ip IP       Listening ip
  -p PORT, --port PORT  Listening port
  -rf, --run-forever    Disable auto termination of server
  -mi MAX_IDLE, --max-idle MAX_IDLE
                        Maximum idle time before auto termination (in seconds). Default 1 hour.
  -o LOG, --log LOG     Log into file
```
