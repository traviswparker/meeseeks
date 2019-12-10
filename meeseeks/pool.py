#!/usr/bin/env python3

import time
import threading
import logging

from .task import Task

class Pool(threading.Thread):
    '''job queue manager'''
    def __init__(self,__node,__pool,__state,**cfg):
        self.node=__node #node we are running on
        self.pool=__pool #pool we service
        self.state=__state #state thread
        threading.Thread.__init__(self,daemon=True,name=self.node+'.Pool.'+self.pool,target=self.__pool_run)
        self.logger=logging.getLogger(self.name)
        self.shutdown=threading.Event()
        self.__tasks={} #map of job_id -> Task object
        self.config(**cfg)
        self.start()

    def config(self,refresh=1,update=30,slots=0,runtime=0,**cfg):
        if refresh: self.refresh=int(refresh) #how often we update the job q
        if update: self.update=int(update) #how often we update the state of running/waiting jobs
        if runtime: self.max_runtime=int(runtime)
        else: self.max_runtime=None
        if slots: self.slots=int(slots) #number of job slots, or True if not limited
        else: self.slots=True

    def start_job(self,jid):
        '''caaaaaaan do!'''
        job=self.state.get_job(jid)
        try: 
            self.__tasks[jid]=Task(job)
            pid=self.__tasks[jid].pid
            self.state.update_job(jid,
                state='running',
                start_ts=time.time(),
                pid=pid,
                start_count=job['start_count']+1
            )
            self.logger.info('started %s [%s]'%(jid,pid))
        except Exception as e:
            self.logger.warning(e,exc_info=True)
            self.state.update_job(jid,state='failed',error=str(e))
    
    def kill_job(self,jid,job):
        '''kill running task and wait for task to exit'''
        self.logger.info('killing %s [%s]'%(jid,self.__tasks[jid].pid))
        try:
            self.__tasks[jid].kill()
            self.__tasks[jid].join()
        except Exception as e: self.logger.warning(e,exc_info=True)
        self.state.update_job(jid,state='killed')

    def check_job(self,jid,job):
        state=job['state']
        rc=self.__tasks[jid].poll()
        if rc is not None: #task exited
            fail_count=job['fail_count']
            if state == 'running':
                if rc == 0: state='done'
                else: 
                    state='failed'
                    fail_count+=1
            self.state.update_job( jid,
                state=state,
                end_ts=time.time(),
                rc=rc, 
                pid=None, 
                fail_count=fail_count,
                stdout_data=self.__tasks[jid].stdout_data,
                stderr_data=self.__tasks[jid].stderr_data )
            self.logger.info("job %s %s (%s)"%(jid,state,rc))
            del self.__tasks[jid] #free the slot
        #was job killed or max runtime
        elif job.get('runtime') and (time.time()-job['start_ts'] > job['runtime']):
            self.logger.warning('job %s exceeded job runtime of %s'%(jid,job['runtime']))
            self.kill_job(jid,job)
        elif self.max_runtime and (time.time()-job['start_ts'] > self.max_runtime):
            self.logger.warning('job %s exceeded pool runtime of %s'%(jid,self.max_runtime))
            self.kill_job(jid,job)

    def __pool_run(self):
        while not self.shutdown.is_set():
            try:
                #get jobs assigned to this node and pool
                pool_jobs=self.state.get(node=self.node,pool=self.pool)
                for jid,job in pool_jobs.items():
                    #check running jobs
                    if jid in self.__tasks:
                        if job['state'] == 'killed': self.kill_job(jid,job)
                        self.check_job(jid,job)
                    elif (job['state'] == 'running'): #job is supposed be running but isn't?
                        self.logger.warning('job %s in state running but no task'%jid)
                        self.state.update_job( jid, state='failed', error='crashed' )
                    #update queued/running jobs
                    if (job['state'] == 'running' or job['state'] == 'waiting') \
                        and (time.time()-job['ts'] > self.update):
                            self.state.update_job(jid) #touch it so it doesn't expire
                    #check to see if we can start a job
                    elif job['state'] == 'new' or job['state'] == 'waiting' \
                        or (job['state'] == 'done' and job.get('restart')) \
                        or (job['state'] == 'failed' and job.get('fail_count') < int(job.get('retries',0)) ):
                            if not job.get('hold') and (not self.slots or (len(self.__tasks) < self.slots)):
                                 self.start_job(jid)
                            #job is waiting for a slot
                            elif job['state'] != 'waiting': self.state.update_job(jid,state='waiting')
                #check for orphaned tasks. This shouldn't happen but it can if time jumps.
                for jid in list(self.__tasks.keys()):
                    if jid not in pool_jobs:
                        self.logger.warning('task %s not found in pool jobs'%jid)
                        self.kill_job(jid,None) #just kill it
                        del self.__tasks[jid] #recover the slot
                #update pool status with free slots
                if self.slots: 
                    slots_free=self.slots-len(self.__tasks)
                    if slots_free < 0: slots_free=0 #can happen if slots changed
                else: slots_free=None #no slots set
                self.state.update_pool_status(self.pool,self.node,slots_free)
            except Exception as e: self.logger.error(e,exc_info=True)
            time.sleep(self.refresh)

        #at shutdown, kill all jobs, mark as failed
        pool_jobs=self.state.get(node=self.node,pool=self.pool)
        for jid in list(self.__tasks.keys()):
            job=pool_jobs[jid]
            self.kill_job(jid,job)
            del self.__tasks[jid] 
            self.state.update_job( jid,
                state='failed',
                error='shutdown',
                pid=None )

        #at shutdown remove self from pool status
        self.state.update_pool_status(self.pool,self.node,False)