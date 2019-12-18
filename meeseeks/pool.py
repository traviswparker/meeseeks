#!/usr/bin/env python3

import time
import threading
import logging

from .task import Task

'''PLUGIN API
    The Pool class can be inherited to create a pool that does something other than spawn processes.
    To do this, replace the Task class.

    import threading
    from meeseeks.pool import Pool
    
    class MyTask:
        ...(see task module for details)...

    class MyPool(Pool): 
        POOL_TYPE='MyPool'
        TASK_CLASS=MyTask
        #we don't change anything else in Pool

'''

class Pool(threading.Thread):
    '''job queue manager'''
    POOL_TYPE='Pool'
    TASK_CLASS=Task

    def __init__(self,__node,__pool,__state,**cfg):
        self.node=__node #node we are running on
        self.pool=__pool #pool we service
        self.state=__state #state thread
        threading.Thread.__init__(self,daemon=True,name=self.node+'.'+self.POOL_TYPE+'.'+self.pool,target=self.__pool_run)
        self.logger=logging.getLogger(self.name)
        self.shutdown=threading.Event()
        self.__tasks={} #map of job_id -> Task object
        self.config(**cfg)
        self.start()

    def config(self,update=30,slots=None,runtime=None,**cfg):
        if update: self.update=int(update) #how often we update the state of running jobs
        if runtime: self.max_runtime=int(runtime)
        else: self.max_runtime=None
        if slots is not None: #number of job slots (>0:set slots, 0:not defined, <0 drains pool)
            if slots==0: self.slots=None
            else: self.slots=int(slots) 

    def update_job(self,jid,**data):
        '''hook for state.update_job that sets active flag'''
        if 'state' in data: #set active flag to match state
            if data['state'] in self.state.JOB_INACTIVE: data['active']=False
            else: data['active']=True
        return self.state.update_job(jid,**data)

    def start_job(self,jid):
        '''caaaaaaan do!'''
        job=self.state.get_job(jid)
        try: 
            self.__tasks[jid]=self.TASK_CLASS(job)
            self.update_job(jid,
                state='running',
                start_ts=time.time(),
                start_count=job['start_count']+1, 
                **self.__tasks[jid].info
            )
            self.logger.info('started %s [%s]'%(jid,self.__tasks[jid].name))
        except Exception as e:
            self.logger.warning(e,exc_info=True)
            self.update_job(jid,state='failed',error=str(e))
    
    def kill_job(self,jid,job):
        '''kill running task and wait for task to exit'''
        self.logger.info('killing %s [%s]'%(jid,self.__tasks[jid].name))
        try:
            self.__tasks[jid].kill()
            self.__tasks[jid].join()
        except Exception as e: self.logger.warning(e,exc_info=True)
        self.update_job(jid,state='killed',**self.__tasks[jid].info)

    def check_job(self,jid,job):
        state=job['state']
        #r will be None if running, True if success, False if failure
        info=self.__tasks[jid].info
        r=self.__tasks[jid].poll()
        if r is not None: #task exited
            fail_count=job['fail_count']
            if state == 'running':
                if r: state='done'
                else: 
                    state='failed'
                    fail_count+=1
            self.update_job( jid,
                state=state,
                end_ts=time.time(),
                fail_count=fail_count, 
                **self.__tasks[jid].info)
            self.logger.info("job %s %s"%(jid,state))
            del self.__tasks[jid] #free the slot
        #was job killed or max runtime
        elif job.get('runtime') and (time.time()-job['start_ts'] > job['runtime']):
            self.logger.warning('job %s exceeded job runtime of %s'%(jid,job['runtime']))
            self.kill_job(jid,job)
        elif self.max_runtime and (time.time()-job['start_ts'] > self.max_runtime):
            self.logger.warning('job %s exceeded pool runtime of %s'%(jid,self.max_runtime))
            self.kill_job(jid,job)
        return info #return task info if any

    def __pool_run(self):
        while not self.shutdown.is_set():
            try:
                #get jobs assigned to this node and pool
                pool_jobs=self.state.get(node=self.node,pool=self.pool)
                for jid,job in pool_jobs.items():
                    
                    #check running jobs
                    task_info={} #task info if running
                    if jid in self.__tasks:
                        if job['state'] == 'killed': self.kill_job(jid,job)
                        task_info=self.check_job(jid,job)
                    elif (job['state'] == 'running'): #job is supposed be running but isn't?
                        self.logger.warning('job %s in state running but no task'%jid)
                        self.update_job( jid, state='failed', error='crashed' )
                    elif job['state'] == 'killed' and job.get('active'): #reset killed to set active=False
                        self.update_job(jid,state='killed')
                    
                    #update active jobs, will also set job nactive if not in an active state
                    if job.get('active') and (time.time()-job['ts'] > self.update):
                            self.update_job(jid,state=job['state'],**task_info) #touch it so it doesn't expire
                    
                    #can we activate a job?
                    # new and not active, 
                    # or done and restart set, 
                    # or failed and retries not exceeded)
                    elif (job['state'] == 'new' and not job.get('active')) \
                        or (job['state'] == 'done' and job.get('restart')) \
                        or (job['state'] == 'failed' and job.get('fail_count') < int(job.get('retries',0)) ):
                            self.update_job(jid,active=True) #claim the job
                            #do we have a free slot, and is the job not on hold?
                            if not job.get('hold') and (not self.slots or (len(self.__tasks) < self.slots)):
                                 self.start_job(jid)

                #check for orphaned tasks. This shouldn't happen but it can if time jumps.
                for jid in list(self.__tasks.keys()):
                    if jid not in pool_jobs:
                        self.logger.warning('task %s not found in pool jobs'%jid)
                        self.kill_job(jid,None) #just kill it
                        del self.__tasks[jid] #recover the slot
                        
                #update pool status with free slots
                if self.slots: slots_free=self.slots-len(self.__tasks)
                else: slots_free=None #no slots defined
                self.state.update_pool_status(self.pool,self.node,slots_free)

            except Exception as e: self.logger.error(e,exc_info=True)
            time.sleep(1)

        #at shutdown, kill all jobs, mark as failed
        pool_jobs=self.state.get(node=self.node,pool=self.pool)
        for jid in list(self.__tasks.keys()):
            job=pool_jobs[jid]
            self.kill_job(jid,job)
            self.update_job( jid, state='failed', error='shutdown')

        #at shutdown remove self from pool status
        self.state.update_pool_status(self.pool,self.node,False)