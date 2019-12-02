#!/usr/bin/env python3

import time
import threading
import logging
import uuid

class State(threading.Thread):
    '''cluster state interface
        job submit spec is:
            id: [optional] manually set id of the job, a UUID will be generated if not specified
            pool: <required> the pool the job runs in
            args: <required> list of [ arg0 (executable) [,arg1,arg2,...] ] (command line for job)
            nodelist: [optional] list of nodes to prefer
            stdin: filename to redirect stdin from
            stdout: filename to redirect stdout to
            stderr: filename to redirect stderr to
            restart_on_done: if true, job will be restart on exit
            restart_on_fail: if true, job will be reassigned to the nodelist if provided and restart on failure
            max_runtime: job maximum runtime in seconds
        job attributes:
            node: the node the job is assigned to
            state: the job state (new,waiting,running,done,failed)
            rc: exit code if job done/failed
            error: error details if job did not spawn
            stdout: base64 encoded stdout after exit if not redirected to a file
            stdout: base64 encoded stderr after exit if not redirected to a file
            ts: update timestamp
            submit_ts: submit timestamp
            start_ts: job start timestamp
            end_ts: job end timestamp
    '''

    #only allow these keys to prevent shenanigans
    JOB_SPEC=[  'id',
                'pool',
                'args',
                'nodelist',
                'stdin',
                'stderr',
                'stdout',
                'restart_on_done',
                'restart_on_fail',
                'max_runtime'
            ]

    def __init__(self,node=None,refresh=1,expire=60,expire_active_jobs=True):
        self.node=node
        name='State'
        if self.node: name=self.node+'.'+name
        threading.Thread.__init__(self,daemon=True,name=name,target=self.__state_run)
        self.logger=logging.getLogger(self.name)

        self.shutdown=threading.Event()

        self.refresh=refresh
        self.expire=expire
        self.expire_active_jobs=expire_active_jobs
    
        self.__lock=threading.Lock() #lock on __jobs dict
        self.__jobs={} #(partial) cluster job state, this is private because we lock during any changes
        self.__pool_status={} #map of [pool][node][open slots] for nodes we connect downstream to
        self.__node_status={} #map of node:last status

        self.start()

    #these return a copy of the private state, use update_ methods to modify it
    def get_pool_status(self): 
        '''get a pool:node:slots_free map of pool availability'''
        return self.__pool_status.copy()

    def get_node_status(self): 
        '''get a node:status map of cluster status'''
        return self.__node_status.copy()
    
    def update_pool_status(self,pool,node,slots): 
        with self.__lock:
            self.__pool_status.setdefault(pool,{})[node]=slots

    def update_node_status(self,node,**node_status): 
        with self.__lock:
            self.__node_status.setdefault(node,{}).update(**node_status)
            #take offline nodes out of the pools
            if not node_status.get('online'):
                for pool in self.__pool_status.keys():
                    if node in self.__pool_status[pool]:
                        del self.__pool_status[pool][node]

    def get(self,node=None,pool=None,ts=None):
        '''dump all state for a node/pool/or updated after a certain ts'''
        with self.__lock:
            try: 
                return dict((jid,job.copy()) for (jid,job) in self.__jobs.items() if \
                    (   (not node or job['node']==node) and \
                        (not pool or job['pool']==pool) and \
                        (not ts or job['ts']>ts) )
                )
            except Exception as e: self.logger.warning(e,exc_info=True)
        return None

    def sync(self,jobs={},status={},remote_node=None):
        '''update local status cache with incoming status 
        and jobs not in local state or job ts >= local jobs ts'''
        with self.__lock:
            updated=[]
            try:
                for jid,job in jobs.items():
                    if jid not in self.__jobs or self.__jobs[jid]['ts'] <= job['ts']:
                        if not job.get('node'): job['node']=self.node #may not be set from client
                        self.__jobs.setdefault(jid,{}).update(job)
                        updated.append(jid)
            except Exception as e: self.logger.warning(e,exc_info=True)
        #update our status from incoming status data
        try:
            for node,node_status in status.get('nodes',{}).items():
                #for the node we are syncing from
                #save the list of nodes it has seen
                if remote_node and node==remote_node: 
                    node_status['seen']=list(status['nodes'].keys())
                self.update_node_status(node,**node_status)
            for pool,nodes in status.get('pools',{}).items():
                for node,slots in nodes.items(): self.update_pool_status(pool,node,slots)
        except Exception as e: self.logger.warning(e,exc_info=True)
        #return updated items
        return updated

    def get_job(self,jid):
        '''return job jid's data from state'''
        with self.__lock:
            try:
                if jid in self.__jobs: return self.__jobs.get(jid).copy()
            except Exception as e: self.logger.warning(e,exc_info=True)
    
    def update_job(self,jid,**data):
        '''update job jid with k/v in data'''
        with self.__lock:
            try:
                if jid in self.__jobs: 
                    self.__jobs[jid].update(ts=time.time(),**data)
                    return self.__jobs.get(jid)
                else: return False
            except Exception as e: self.logger.warning(e,exc_info=True)

    def list_jobs(self,**kwargs):
        '''return list of job ids'''
        return list(self.get(**kwargs).keys())
        
    def add_job(self,**jobargs):
        '''add a job, see job spec for proper key=values'''
        with self.__lock:
            try:
                #filter job spec keys
                jobargs=dict((k,v) for (k,v) in jobargs.items() if k in self.JOB_SPEC)
                if 'pool' not in jobargs or 'args' not in jobargs: return False #jobs have to have a command and pool to run in
                jid=jobargs.get('id',str(uuid.uuid1())) #use preset id or generate one
                if jid in self.__jobs: #job exists
                    self.logger.warning('add_job: %s exists'%jid)
                    return False
                self.__jobs[jid]={  'ts':time.time(),           #last updated timestamp
                                    'submit_ts':time.time(),    #submit timestamp
                                    'pool':None,                #pool to run in 
                                    'nodelist':[],              #list of nodes allowed to handle this job
                                    'node':self.node,           #node we are on
                                    'state':'new'               #job state
                                }             
                self.__jobs[jid].update(jobargs)
                return jid
            except Exception as e: self.logger.warning(e,exc_info=True)

    def __state_run(self):
        self.logger.info('started')
        while not self.shutdown.is_set():
            self.logger.debug('status %s'%self.__node_status)
            self.logger.debug('pools %s'%self.__pool_status)
            #look for jobs that should have been updated
            with self.__lock:
                try:
                    for jid,job in self.__jobs.copy().items():
                        if time.time()-job['ts'] > self.expire: 
                            #jobs that have ended for some reason will no longer be updated, so expire them.
                            if job['state'] in ['done','killed','failed']:
                                self.logger.debug('expiring job %s'%jid)
                                del self.__jobs[jid]
                            elif self.expire_active_jobs: 
                                #this job *should* have been updated
                                self.logger.warning('job %s not updated in %s seconds'%(jid,self.expire))
                                #if we restart on fail and have a nodelist
                                if job.get('restart_on_fail') and job['nodelist']:
                                    # try kicking it back to the first node for rescheduling
                                    self.__jobs[jid].update(node=job['nodelist'][0]) 
                                #set job to failed, it might restart if it can
                                self.__jobs[jid].update(ts=time.time(),state='failed',error='expired')
                                
                                
                except Exception as e: self.logger.warning(e,exc_info=True)
            #set nodes that have not sent status to offline
            for node,node_status in self.__node_status.items():
                if node_status.get('online') and time.time()-node_status['ts'] > self.expire:
                    self.logger.warning('node %s not updated in %s seconds'%(node,self.expire))
                    self.update_node_status(node,online=False)

            time.sleep(self.refresh)