import sys
import shutil
import math


class Progress:
    def __init__(self, max_steps, output=sys.stdout, disable_stdout=True):
        self.steps = 0
        self.max_steps = max_steps
        self.current_step = None
        self.output = sys.stdout
        self.line_width = 0
        if disable_stdout:
            sys.stdout = open('/dev/null', 'w')
            sys.stderr = open('/dev/null', 'w')


    def __write(self, s):
        self.output.write(s)
        self.line_width = len(s)


    def step_start(self, name):
        self.__write(f'{name} ...\033[K\n')
        self.current_step = name

        self.render_bar()


    def step_finish(self):
        self.steps += 1
        self.render_bar()


    def step_status(self, error=False, status=None):
        CHECK_MARK='\033[0;32m\u2714\033[0m'
        RED_CROSS='\033[0;31m\u2718\033[0m'
        INFO = '\033[1;33m=>\033[0m'
        
        width, _ = shutil.get_terminal_size((80, 20))
        for _ in range(math.ceil(self.line_width / width)):
            self.output.write('\033[1A\033[K')  # 1 up, clear line

        if status:
            self.__write(f'{self.current_step} {INFO} {status}\033[K\n')
            self.output.write('\033[K')
            self.line_width = 0
            self.render_bar()
        elif not error:
            self.__write(f'{self.current_step} {CHECK_MARK}\033[K\n')
        else:
            self.__write(f'{self.current_step} {RED_CROSS} => {error}\033[K\n')
        
    
    def render_bar(self):
        width, _ = shutil.get_terminal_size((80, 20))

        percent = self.steps/self.max_steps
        steps_width = len(str(self.max_steps))
        prefix = f'Progress: [{self.steps:{steps_width}d}/{self.max_steps} {percent:4.0%}]'

        if len(prefix)+8 > width:
          self.output.write('\033[0;31mWidth too small!\033[0m\n')
          return

        width_left = width - len(prefix) - 3
        hashes = math.floor(width_left*percent)
        dots = (width_left-hashes)
        suffix = f'[{"#"*hashes}{"."*dots}]'

        progress = f'\033[0;42;30m{prefix}\033[0m {suffix}'
        self.output.write('\033[K\n')
        self.output.write(progress)
        if self.steps < self.max_steps:
            self.output.write('\033[1A\r')  #1 up, start
        self.output.flush()
    



import time


max_steps = 5
p = Progress(max_steps)

for i in range(max_steps):
    pad = '~'*i*100
    p.step_start(f'test_{i}_{pad}',)

    #if i % 2 == 1:
    # if i == max_steps - 1:
    #     p.step(error="ASDF")

    time.sleep(1.0)
    p.step_status(error="foobar")
    time.sleep(0.5)
    if i % 2 == 1:
        p.step_finish()
        continue
    for _ in range(3):
        p.step_status(status=f"Retry {_}")
        time.sleep(0.5)
    p.step_status(error=False)
    p.step_finish()
    time.sleep(1.5)


# p.finish()