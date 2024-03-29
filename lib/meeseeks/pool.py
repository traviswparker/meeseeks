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

    def __init__(self,node,pool,state,**cfg):
        self.node=node #node we are running on
        self.pool=pool #pool we service
        self.state=state #state thread
        threading.Thread.__init__(self,daemon=True,name=self.node+'.'+self.POOL_TYPE+'.'+self.pool,target=self.__pool_run)
        self.logger=logging.getLogger(self.name)
        self.shutdown=threading.Event()
        self.__tasks={} #map of job_id -> Task object
        self.config(**cfg)
        self.start()

    def config(self,slots=0,update=None,runtime=None,drain=False,hold=False,**cfg):
        if update: self.update=int(update) #how often we update the state of running jobs
        else: self.update=None
        if runtime: self.max_runtime=int(runtime)
        else: self.max_runtime=None
        if slots==0: self.slots=True
        else: self.slots=int(slots) 
        if drain: self.slots=0 #set free slots to 0 to avoid new jobs
        self.hold=hold

    def update_job(self,jid,**data):
        '''hook for state.update_job that sets active flag'''
        if 'state' in data: #set active flag to match state
            if data['state'] in self.state.JOB_INACTIVE: data['active']=False
            else: data['active']=True
        return self.state.update_job(jid,**data)

    def start_job(self,jid):
        '''caaaaaaan do!'''
        job=self.state.get_job(jid)
        job.update(id=jid) #put jid in job spec for task class
        try: 
            self.__tasks[jid]=self.TASK_CLASS(job)
            self.update_job(jid,
                state='running',
                start_ts=time.time(),
                start_count=job['start_count']+1, 
                **self.__tasks[jid].info
            )
            self.logger.info('job %s started %s',jid,self.__tasks[jid].name)
        except Exception as e:
            self.logger.warning(e)
            self.update_job(jid,state='failed',error=str(e))
    
    def kill_job(self,jid,job,state='killed'):
        '''kill running task and wait for task to exit'''
        info={}
        if jid in self.__tasks:
            self.logger.debug('killing %s %s',jid,self.__tasks[jid].name)
            info=self.__tasks[jid].info
            try:
                self.__tasks[jid].kill()
                self.__tasks[jid].join()
            except Exception as e: self.logger.warning(e,exc_info=True)
        return self.update_job(jid,state=state,**info)

    def __pool_run(self):
        while not self.shutdown.is_set():
            try:
                #get jobs assigned to this node and pool
                pool_jobs=self.state.get(node=self.node,pool=self.pool)
                for jid,job in sorted(pool_jobs.items(),key=lambda j:j[1]['submit_ts']):
                    #check running jobs
                    if jid in self.__tasks: 
                        state=job['state']
                        #r will be None if running, True if success, False if failure
                        info,r=self.__tasks[jid].info,self.__tasks[jid].poll()
                        if r is not None: #task exited
                            fail_count=job.get('fail_count',0)
                            if state == 'running': #only update state if running, not if killed
                                if r: state='done'
                                else: 
                                    state='failed'
                                    fail_count+=1
                            #this looks odd but what we are doing is updating the job and the thread's copy of the job
                            #otherwise it can get set back to running if the job ends right at the thread's update tick
                            job.update(
                                self.update_job( jid,
                                    state=state,
                                    end_ts=time.time(),
                                    fail_count=fail_count, 
                                    **info )
                            )
                            self.logger.info("task %s %s",jid,state)
                            del self.__tasks[jid] #free the slot
                        #did job exceed max runtime? if so, kill job but mark as failed
                        elif job.get('runtime') and (time.time()-job['start_ts'] > job['runtime']):
                            self.logger.warning('job %s exceeded job runtime of %s',jid,job['runtime'])
                            self.kill_job(jid,job,'failed')
                        elif self.max_runtime and (time.time()-job['start_ts'] > self.max_runtime):
                            self.logger.warning('job %s exceeded pool runtime of %s',jid,self.max_runtime)
                            self.kill_job(jid,job,'failed')
                        #job can get stuck in new if it is reset while running, fix the state
                        elif job['state'] == 'new':
                            job.update( 
                                self.update_job( jid, 
                                state='running', 
                                **self.__tasks[jid].info )
                            )
                    elif (job['state'] == 'running'): #job is supposed be running but isn't?
                        self.logger.warning('job %s in state running but no task exists',jid)
                        job=self.update_job( jid, state='failed', error='task' )
                    #kill or update active jobs
                    if job.get('active'):
                        if job['state'] == 'killed': job=self.kill_job(jid,job) #kill job if requested
                        elif self.update and (time.time()-job['ts'] > self.update): #if update interval
                            self.update_job(jid,state=job['state'],**info) #update with task info
                    
                    #can we activate a job?
                    #  set job active if not
                    #  start job if not on hold and a slot is free
                    if job['state'] == 'new':
                        #do we have a free slot, and is the job/pool not on hold?
                        if not job.get('hold') and not self.hold and ((self.slots is True) or (len(self.__tasks) < self.slots)):
                            self.start_job(jid) #start it
                        #if on hold in pool, claim it without running it yet
                        else: job=self.update_job(jid,active=True) #activate it

                #check for orphaned tasks. This shouldn't happen but it can if time jumps.
                for jid in list(self.__tasks.keys()):
                    if jid not in pool_jobs:
                        self.logger.warning('job %s not in state',jid)
                        self.kill_job(jid,None) #just kill it
                        del self.__tasks[jid] #recover the slot
                        
                #update pool status
                self.state.update_pool(self.pool,self.node,self.slots)

            except Exception as e: self.logger.error(e,exc_info=True)
            time.sleep(1)

        #at shutdown, kill all jobs, mark as failed
        pool_jobs=self.state.get(node=self.node,pool=self.pool)
        for jid in list(self.__tasks.keys()):
            job=pool_jobs[jid]
            self.kill_job(jid,job)
            self.update_job( jid, state='failed', error='pool')

        #at shutdown remove self from pool status
        self.state.update_pool(self.pool,self.node,False)
