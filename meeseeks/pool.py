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

    def config(self,update=30,slots=0,runtime=None,drain=False,hold=False,**cfg):
        self.update=int(update) #how often we update the state of running jobs
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
        try: 
            self.__tasks[jid]=self.TASK_CLASS(job)
            self.update_job(jid,
                state='running',
                start_ts=time.time(),
                start_count=job['start_count']+1, 
                **self.__tasks[jid].info
            )
            self.logger.info('task %s started [%s]'%(jid,self.__tasks[jid].name))
        except Exception as e:
            self.logger.warning(e)
            self.update_job(jid,state='failed',error=str(e))
    
    def kill_job(self,jid,job,state='killed'):
        '''kill running task and wait for task to exit'''
        task_info={}
        if jid in self.__tasks:
            self.logger.debug('killing %s [%s]'%(jid,self.__tasks[jid].name))
            task_info=self.__tasks[jid].info
            try:
                self.__tasks[jid].kill()
                self.__tasks[jid].join()
            except Exception as e: self.logger.warning(e,exc_info=True)
        return self.update_job(jid,state=state,**task_info)

    def check_job(self,jid,job):
        state=job['state']
        #r will be None if running, True if success, False if failure
        info=self.__tasks[jid].info
        r=self.__tasks[jid].poll()
        if r is not None: #task exited
            fail_count=job.get('fail_count',0)
            if state == 'running':
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
                    **self.__tasks[jid].info )
            )
            self.logger.info("task %s %s"%(jid,state))
            del self.__tasks[jid] #free the slot
        #job can get stuck in new if it is reset while running, fix the state
        elif job['state'] == 'new':
            job.update( 
                self.update_job( jid, 
                state='running', 
                **self.__tasks[jid].info )
            )
        #did job exceed max runtime? if so, kill job but mark as failed
        elif job.get('runtime') and (time.time()-job['start_ts'] > job['runtime']):
            self.logger.warning('job %s exceeded job runtime of %s'%(jid,job['runtime']))
            self.kill_job(jid,job,'failed')
        elif self.max_runtime and (time.time()-job['start_ts'] > self.max_runtime):
            self.logger.warning('job %s exceeded pool runtime of %s'%(jid,self.max_runtime))
            self.kill_job(jid,job,'failed')
        return info #return task info if any

    def __pool_run(self):
        while not self.shutdown.is_set():
            try:
                #get jobs assigned to this node and pool
                pool_jobs=self.state.get(node=self.node,pool=self.pool)
                for jid,job in pool_jobs.items():
                    
                    #check running jobs
                    task_info={} #task info if running
                    if jid in self.__tasks: task_info=self.check_job(jid,job)
                    elif (job['state'] == 'running'): #job is supposed be running but isn't?
                        self.logger.warning('job %s in state running but no task'%jid)
                        job=self.update_job( jid, state='failed', error='crashed' )
                    #periodically update active jobs so they don't expire
                    if job.get('active'):
                        if job['state'] == 'killed': job=self.kill_job(jid,job) #kill job if requested
                        elif (time.time()-job['ts'] > self.update): #if update interval expired
                            self.update_job(jid,state=job['state'],**task_info) #update with task info
                    
                    #can we activate a job?
                    #  set job active if not
                    #  start job if not on hold and a slot is free
                    if job['state'] == 'new':
                        if not job.get('active'): job=self.update_job(jid,active=True) #claim the job
                        #do we have a free slot, and is the job/pool not on hold?
                        if not job.get('hold') and not self.hold and ((self.slots is True) or (len(self.__tasks) < self.slots)):
                                self.start_job(jid)

                #check for orphaned tasks. This shouldn't happen but it can if time jumps.
                for jid in list(self.__tasks.keys()):
                    if jid not in pool_jobs:
                        self.logger.warning('task %s not found in pool jobs'%jid)
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
            self.update_job( jid, state='failed', error='shutdown')

        #at shutdown remove self from pool status
        self.state.update_pool(self.pool,self.node,False)