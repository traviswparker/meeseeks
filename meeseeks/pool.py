#!/usr/bin/env python3

import time
import threading
import logging

from .task import Task

class Pool(threading.Thread):
    '''job queue manager'''
    def __init__(self,node,pool,state,refresh=1,update=30,slots=None,max_runtime=None):
        self.node=node #node we are running on
        self.pool=pool #pool we service
        self.state=state #state thread
        self.refresh=refresh #how often we update the job q
        self.update=update #how often we update the state of running/waiting jobs
        self.max_runtime=max_runtime
        self.slots=slots #number of job slots, or None if not limited
        threading.Thread.__init__(self,daemon=True,name='Pool.'+self.pool,target=self.__pool_run)
        self.logger=logging.getLogger(self.name)
        self.shutdown=threading.Event()
        self.__tasks={} #map of job_id -> Task object
        self.start()

    def start_job(self,jid):
        job=self.state.get_job(jid)
        try: 
            self.__tasks[jid]=Task(job)
            pid=self.__tasks[jid].pid
            self.state.update_job(jid,
                state='running',
                start_ts=time.time(),
                pid=pid,
            )
            self.logger.info('started %s [%s]'%(jid,pid))
        except Exception as e:
            self.logger.warning(e,exc_info=True)
            self.state.update_job(jid,state='failed',error=str(e))
    
    def kill_job(self,jid,job):
        self.logger.info('killing %s [%s]'%(jid,job['pid']))
        self.__tasks[jid].kill()
        self.state.update_job(jid,state='killed')

    def check_job(self,jid,job):
        state=job['state']
        rc=self.__tasks[jid].poll()
        if rc is not None: #job exited
            self.__tasks[jid].join() #wait for task to exit
            if state == 'running':
                if rc == 0: state='done'
                else: state='failed'
            self.state.update_job( jid,
                state=state,
                end_ts=time.time(),
                rc=rc, pid=None,
                stdout=self.__tasks[jid].stdout,
                stderr=self.__tasks[jid].stderr )
            self.logger.info("job %s %s (%s)"%(jid,state,rc))
            del self.__tasks[jid] #and dispose of it
        #was job killed or max runtime
        elif job.get('max_runtime') and (time.time()-job['start_ts'] > job['max_runtime']):
            self.logger.warning('job %s exceeded job max_runtime of %s'%(jid,job['max_runtime']))
            self.kill_job(jid,job)
        elif self.max_runtime and (time.time()-job['start_ts'] > self.max_runtime):
            self.logger.warning('job %s exceeded pool max_runtime of %s'%(jid,self.max_runtime))
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
                    elif (job['state'] == 'running'): #job is supposed be running but isn't. node crash?
                        self.logger.warning('job %s in state running but no task'%jid)
                        self.state.update_job( jid, state='failed', error='Lost' )
                    #update queued/running jobs
                    if (job['state'] == 'running' or job['state'] == 'waiting') \
                        and (time.time()-job['ts'] > self.update):
                            self.state.update_job(jid) #touch it so it doesn't expire
                    #check to see if we can start a job
                    elif job['state'] == 'new' or job['state'] == 'waiting' \
                        or (job['state'] == 'done' and job.get('restart_on_done')) \
                        or (job['state'] == 'failed' and job.get('restart_on_fail')):
                            if (not self.slots) or (len(self.__tasks) < self.slots): self.start_job(jid)
                            #job is waiting for a slot
                            elif job['state'] != 'waiting': self.state.update_job(jid,state='waiting')
            except Exception as e: self.logger.error(e,exc_info=True)
            
            #update pool status with free slots
            if self.slots: slots_free=self.slots-len(self.__tasks)
            else: slots_free=None #no slots set
            self.state.update_pool_status(self.pool,self.node,slots_free)

            time.sleep(self.refresh)

        #at shutdown, kill all jobs
        pool_jobs=self.state.get(node=self.node,pool=self.pool)
        for jid in list(self.__tasks.keys()):
            job=pool_jobs[jid]
            self.kill_job(jid,job)
            self.check_job(jid,job)