#!/usr/bin/env python3

import threading
import logging
import subprocess
import base64

class Task(threading.Thread):
    '''subprocess manager'''        
    def __init__(self,job):
        threading.Thread.__init__(self)
        popen_args={}
        self.stdin=self.stdout=self.stderr=None #file handles if redirecting
        #stdin from file
        stdin=job.get('stdin')
        if stdin:
            self.stdin=open(stdin,'rb')
            popen_args.update(stdin=self.stdin)
        #stdout to file, or to pipe if True
        stdout=job.get('stdout')
        if stdout is True: popen_args.update(stdout=subprocess.PIPE)
        elif stdout: 
            self.stdout=open(stdout,'ab')
            popen_args.update(stdout=self.stdout)
        #stderr to file, or pipe
        stderr=job.get('stderr')
        if stderr is True: popen_args.update(stderr=subprocess.PIPE)
        elif stderr: 
            self.stderr=open(stderr,'ab')
            popen_args.update(stderr=self.stderr)
        #start subprocess
        self.__sub=subprocess.Popen(job.get('cmd'),**popen_args)
        self.pid=self.__sub.pid
        self.start() #thread will wait on subprocess
    
    def run(self): 
        #block here until process finishes
        stdout,stderr=self.__sub.communicate()
        #close file handles
        if self.stdin: self.stdin.close()
        if self.stdout: self.stdout.close()
        if self.stderr: self.stderr.close()
        #return output as a base64 string if we got any
        if stdout: self.stdout=base64.b64encode(stdout).decode()
        else: self.stdout=None
        if stderr: self.stderr=base64.b64encode(stderr).decode()
        else: self.stderr=None

    def kill(self): self.__sub.kill()
    def poll(self): return self.__sub.poll()