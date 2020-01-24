#!/usr/bin/env python3

import threading
import subprocess
import base64
from meeseeks.util import *

'''PLUGIN API
    Plugins should implement an alternate Task class
    Tasks do a job, then they stop existing
    Tasks should be self-starting and must not block the Pool thread.
    The Pool will join() the task thread when task.poll() indicates an exit
    
    class Task(threading.Thread):
        
        def __init__(self,job): #we're passed a job dict but we can't update it
            self.info={} #the Pool will use Task.info to update the job dict
            ...start the task thread here and update info...

        def kill(self):
            #this should stop the task thread and update info (if the thread doesn't at exit)

        def poll(self):
            #this should poll the task thread and return
            None: task is running
            True: task finished normally
            False: task failed
'''

class Task(threading.Thread):
    '''subprocess manager'''        
    def __init__(self,job):
        popen_args={}
        popen_args.update(job.get('config',{})) #add config if any
        self.info={} #info to update job
        self.stdin=self.stdout=self.stderr=None #file handles if redirecting
        self.stdout_data=self.stderr_data=None #output if capturing
        self.uid,self.gid=job.get('uid'),job.get('gid')

        # stdin from file
        stdin=job.get('stdin')
        if stdin:
            self.stdin=open(stdin,'rb')
            popen_args.update(stdin=self.stdin)

        # stdout to whatever is passed in, else defaults to PIPE
        stdout=job.get('stdout')
        if stdout: 
            self.stdout=open(stdout,'ab')
            popen_args.update(stdout=self.stdout)
        else:
            popen_args.update(stdout=subprocess.PIPE)

        # stderr to whatever is passed in, else defaults to PIPE
        stderr=job.get('stderr')
        if stderr: 
            self.stderr=open(stderr,'ab')
            popen_args.update(stderr=self.stderr)
        else:
            popen_args.update(stderr=subprocess.PIPE)
            
        # start subprocess. If this raises we don't start a thread
        self.__sub=subprocess.Popen( job.get('args'), 
                    preexec_fn=su(self.uid,self.gid,sub=True),
                    **popen_args)
        self.info['pid']=self.__sub.pid #set pid in job
        #thread will wait on subprocess
        threading.Thread.__init__(self,name=self.__sub.pid,target=self.__task_run)
        self.start() 
    
    def __task_run(self): 
        # block here until process finishes
        stdout,stderr=self.__sub.communicate()

        #clear pid and set rc
        self.info['pid']=None
        self.info['rc']=self.__sub.poll()

        # close file handles
        if self.stdin: self.stdin.close()
        if self.stdout: self.stdout.close()
        if self.stderr: self.stderr.close()

        # return output as a base64 string if we got any
        if stdout: self.info['stdout_data']=base64.b64encode(stdout).decode()
        if stderr: self.info['stderr_data']=base64.b64encode(stderr).decode()

    def kill(self,sig=9): 
        #kill the subprocess
        #we spawn another subprocess to do this
        #(we might need to switch back to root and then to the job user, 
        # and we don't want to change the euid/egid of the parent)
        subprocess.Popen( ['kill','-%s'%sig,'%s'%self.__sub.pid], 
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            preexec_fn=su(self.uid,self.gid,sub=True) ).communicate()

    def poll(self): 
        #return None until the thread exits so we can reliably capture output
        if self.is_alive(): return None
        if self.__sub.poll() is None: self.__sub.kill() #thread is dead but subprocess is not, kill it
        return (not self.__sub.poll()) #return True if success and False if failure