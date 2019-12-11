#!/usr/bin/env python3

from meeseeks.pool import Pool

import time
import threading

class PlumbusTask(threading.Thread):
    def __init__(self,job):
        self.info={'about':'a regular old plumbus'}
        self.duration=int(job.get('args',[0])[0])
        self.killed=False

        threading.Thread.__init__(self)
        self.start()

    def run(self):
        i=0
        while not self.killed and i<self.duration:
            self.info['output']='slept for %s seconds'%i
            time.sleep(1)
            i+=1
        self.info['output']='woke up'

    def kill(self): self.killed=True

    def poll(self):
        if self.is_alive(): return None
        else: return not self.killed
    
class Plumbus(Pool):
    POOL_TYPE='Plumbus'
    TASK_CLASS=PlumbusTask