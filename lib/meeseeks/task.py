#!/usr/bin/env python3

from multiprocessing import Process, Manager
import subprocess
import base64
import os
import logging
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

class Task(Process):
    '''subprocess manager'''        
    def __init__(self,job):
        self.job=job #job spec for task
        self.info=Manager().dict() #task info readable by pool
        #thread will wait on subprocess
        Process.__init__(self,target=self.__task_run)
        self.start() 
    
    def __task_run(self):
        self.logger=logging.getLogger(self.name)
        uid,gid=os.geteuid(),os.getegid() #save current u/g
        try:
            popen_args={}
            popen_args.update(self.job.get('config',{})) #add config if any

            #switch to the user who will be running this job
            su(self.job.get('uid'),self.job.get('gid'))

            # stdin from file
            stdin=self.job.get('stdin')
            if stdin:
                stdin=open(stdin,'rb')
                popen_args.update(stdin=stdin)

            # stdout to whatever is passed in, else defaults to PIPE
            stdout=self.job.get('stdout')
            if stdout: 
                stdout=open(stdout,'ab')
                popen_args.update(stdout=stdout)
            else:
                popen_args.update(stdout=subprocess.PIPE)

            # stderr to whatever is passed in, else defaults to PIPE
            stderr=self.job.get('stderr')
            if stderr: 
                stderr=open(stderr,'ab')
                popen_args.update(stderr=stderr)
            else:
                popen_args.update(stderr=subprocess.PIPE)

            env=self.job.get('env',{}) #get environ as dict
            #set meeseeks env vars
            env.update(
                MEESEEKS_JOB_ID=self.job.get('id',''),
                MEESEEKS_POOL=self.job.get('pool'),
                MEESEEKS_NODE=self.job.get('node'),
                MEESEEKS_SUBMIT_NODE=self.job.get('submit_node'),
                MEESEEKS_TAGS=','.join(t for t in self.job.get('tags',[]) if t is not None)
            )
            
            #kick it! *guitar riff*
            self.__sub=subprocess.Popen( self.job.get('args'), env=env, **popen_args)

            #back to the meeseeks user/group so we can talk to the Manager
            su(uid,gid)
            self.info['pid']=self.__sub.pid #set pid in job
            self.logger.info('started pid %s',self.__sub.pid)

            # block here until process finishes
            stdout_data,stderr_data=self.__sub.communicate()

            #clear pid and set rc
            self.info['pid']=None
            self.info['rc']=self.__sub.poll()

            # close file handles
            if stdin: stdin.close()
            if stdout: stdout.close()
            if stderr: stderr.close()

            # return output as a base64 string if we got any
            if stdout_data: self.info['stdout_data']=base64.b64encode(stdout_data).decode()
            if stderr_data: self.info['stderr_data']=base64.b64encode(stderr_data).decode()

        except Exception as e:
            su(uid,gid) #back to meeseeks user
            self.logger.warning(e,exc_info=True)
            self.info['error']=str(e)

    def kill(self,sig=9):
        #kill the subprocess
        #we spawn another subprocess to do this
        #(we might need to switch back to root and then to the job user, 
        # and we don't want to change the euid/egid of the parent)
        pid=self.info.get('pid')
        self.logger.info('killing pid %s',pid)
        if pid:
            subprocess.Popen( ['kill','-%s'%sig,'%s'%self.info['pid']], 
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            preexec_fn=su(self.job.get('uid'),self.job.get('gid'),sub=True) ).communicate()

    def poll(self): 
        #return None until the thread exits so we can reliably capture output
        if self.is_alive(): return None
        return (not self.info.get('error') and not self.info.get('rc')) #return True if rc=0 and False otherwise
