#!/usr/bin/env python3

import time
import threading
import logging
import uuid
import json

class State(threading.Thread):
    '''cluster state interface
        job submit spec is:
            id: [optional] manually set id of the job, a UUID will be generated if not specified
            pool: <required> the pool the job runs in
            args: <required> list of [ arg0 (executable) [,arg1,arg2,...] ] (command line for job)
            node: [optional] node to run on, job will fail if unavailable
            filter: [optional] pattern to match nodename against, to set preferred nodes
            stdin: filename to redirect stdin from
            stdout: filename to redirect stdout to
            stderr: filename to redirect stderr to
            restart: if true, job will be restart on exit
            restries: count of times to restart a failed job. 
                      job will be reassigned to the start of the nodelist if provided
            runtime: job maximum runtime in seconds
            hold: if true, job will not run until cleared

        job attributes:
            node: the node the job is assigned to
            state: the job state (new,running,done,failed,killed)
            active: True if the job is being processed by a node.
                    To move a job: kill the job, wait for active=False, then reassign and set state='new'.
            rc: exit code if job done/failed
            error: error details if job did not spawn or exit
            stdout: base64 encoded stdout after exit if not redirected to a file
            stdout: base64 encoded stderr after exit if not redirected to a file
            ts: update timestamp
            seq: sync sequence number. Jobs with the highest seq are most recently updated on this node.
            submit_ts: submit timestamp
            start_ts: job start timestamp
            end_ts: job end timestamp
            start_count: count of time job has started
            fail_count: count of times job has failed
    '''

    #only allow these keys to prevent shenanigans
    JOB_SPEC=[  
                'id',
                'pool',
                'args',
                'state',
                'node',
                'filter',
                'stdin',
                'stderr',
                'stdout',
                'restart',
                'retries',
                'runtime',
                'hold'
            ]

    #states of inactive jobs
    JOB_INACTIVE=['done','failed','killed']

    def __init__(self,__node=None,**cfg):
        self.node=__node
        name='State'
        if self.node: name=self.node+'.'+name
        threading.Thread.__init__(self,daemon=True,name=name,target=self.__state_run)
        self.logger=logging.getLogger(self.name)
        self.shutdown=threading.Event()
        self.__lock=threading.Lock() #lock on __jobs dict
        self.__jobs={} #(partial) cluster job state, this is private because we lock during any changes
        self.__pool_status={} #map of [pool][node][open slots] for nodes we connect downstream to
        self.__node_status={} #map of node:last status
        self.__seq=1 #update sequence number. Always increments.
        self.__pool_ts=0 #pool status timestamp, cleaned up every expire
        self.hist_fh=None
        self.__hist_seq=0 #history sequence number.
        self.state_file=None
        self.checkpoint=None
        self.config(**cfg)
        self.__load_state()
        self.start()

    def config(self,expire=60,expire_active_jobs=True,timeout=60,history=None,file=None,checkpoint=None,**cfg):
        with self.__lock:
            if expire: self.expire=int(expire)
            if timeout: self.timeout=int(timeout)
            self.expire_active_jobs=expire_active_jobs
            if file: self.state_file=file
            if checkpoint is not None: self.checkpoint=checkpoint
            if history:
                try: self.hist_fh=open(history,'a')
                except Exception as e: self.logger.warning('%s:%s'%(history,e))
            elif self.hist_fh: 
                self.hist_fh.close()
                self.hist_fh=None

    def write_history(self,jid):
        if self.hist_fh:
            json.dump({jid:self.__jobs[jid]},self.hist_fh)
            self.hist_fh.write('\n')
            self.hist_fh.flush()

    def __save_state(self):
        if self.state_file:
            try:
                with open(self.state_file,'w') as fh: 
                    json.dump(self.__jobs,fh)
                    self.logger.info('saved state to %s'%self.state_file)
            except Exception as e: self.logger.warning('%s:%s'%(self.state_file,e))

    def __load_state(self):
        if self.state_file:
            try:
                with open(self.state_file) as fh: 
                    self.__jobs=json.load(fh)
                    self.logger.info('loaded state from %s'%self.state_file)
            except Exception as e: self.logger.warning('%s:%s'%(self.state_file,e)) 

    #these return a copy of the private state, use update_ methods to modify it
    def get_pool_status(self): 
        '''get a pool:node:slots_free map of pool availability'''
        return self.__pool_status.copy()

    def get_node_status(self): 
        '''get a node:status map of cluster status'''
        return self.__node_status.copy()
    
    def update_pool_status(self,pool,node,slots=None): 
        '''set free slots in pool for node
            if slots < 0 pool is being drained, will not get new jobs
            if slots is 0 pool is full but jobs may wait if other nodes are full
            if slots is None no slots are defined, jobs will be assigned if no other slots available
            if slots is False node will be expired from pool'''
        with self.__lock: self.__update_pool_status(pool,node,slots)
    def __update_pool_status(self,pool,node,slots):
        self.__pool_status.setdefault(pool,{})[node]=slots

    def update_node_status(self,node,**node_status): 
        '''set node status, remove node from pool if node offline'''
        with self.__lock: self.__update_node_status(node,**node_status)
    def __update_node_status(self,node,**node_status): 
        self.__node_status.setdefault(node,{}).update(**node_status)
        #take offline nodes out of the pools
        if not node_status.get('online'):
            for pool in self.__pool_status.keys():
                if node in self.__pool_status[pool] \
                    and self.__pool_status[pool][node] is not False:
                        self.__update_pool_status(pool,node,False)

    def get(self,ids=None,ts=None,seq=None,**query):
        '''dump a list of jobs or all jobs for a node/pool/state/or updated after a certain ts/seq'''
        with self.__lock:
            try: 
                #turn single job id into list
                if ids and type(ids) is not list: ids=[ids]
                #filter by jid list and/or ts/seq greater than
                #do not return jobs without node unless node query
                # (prevents propagation of unrouted jobs)
                r=dict( (jid,job.copy()) for (jid,job) in self.__jobs.items() if \
                        (not ids or jid in ids) \
                        and (not ts or job['ts']>ts) \
                        and (not seq or job['seq']>seq) \
                        and (job['node'] or 'node' in query) ) 
                for (k,v) in query.items(): #filter by arbitrary criteria
                    r=dict((jid,job) for (jid,job) in r.items() if job.get(k)==v)
                return r
            except Exception as e: self.logger.warning(e,exc_info=True)
        return None

    def sync(self,jobs={},status={},remote_node=None):
        '''update local status cache with incoming status 
        and jobs not in local state or job ts >= local jobs ts'''
        with self.__lock:
            updated=[]
            try:
                for jid,job in jobs.items():
                    if jid not in self.__jobs or self.__jobs[jid]['ts'] < job['ts']:
                        self.__update_job(jid,**job)
                        updated.append(jid)
                #update our status from incoming status data
                for node,node_status in status.get('nodes',{}).items():
                    self.__update_node_status(node,**node_status)
                for pool,nodes in status.get('pools',{}).items():
                    for node,slots in nodes.items(): 
                        self.__update_pool_status(pool,node,slots)
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
        '''update job jid with k/v in data and set/clear active flag
        this is ONLY to be used by the agent as no sanity checks are performed'''
        with self.__lock: 
            if jid in self.__jobs: return self.__update_job(jid,**data)
            else: return False
    def __update_job(self,jid,**data): #nolock for internal use
            try:
                if 'seq' in data: del data['seq'] #replace seq but preserve ts if set
                if 'ts' not in data: data['ts']=time.time() #if no timestamp, set current
                self.__jobs.setdefault(jid,{}).update(seq=self.__seq,**data)
                self.__seq+=1
                return self.__jobs.get(jid)
            except Exception as e: self.logger.warning(e,exc_info=True)

    def kill_jobs(self,*args,**kwargs):
        '''kill jobs, args can be a job id, a list of jobids, or a query dict'''
        resp={}
        arg=None
        if kwargs: arg=kwargs
        elif args: arg=args[0]
        if arg: #don't let kill run without an arg
            if type(arg) is list: jids=arg
            elif type(arg) is dict: jids=self.list_jobs(**arg)
            else: jids=args #single job id or list of ids
            for jid in jids: resp[jid]=self.update_job(jid,state='killed')
        return resp
        
    def list_jobs(self,**kwargs):
        '''return list of job ids'''
        return list(self.get(**kwargs).keys())
        
    def submit_job(self,**jobargs):
        '''add or change a job, see job spec for proper key=values'''
        with self.__lock:
            try:
                #if a new state is provided
                state=jobargs.get('state')
                #filter job spec keys
                jobargs=dict((k,v) for (k,v) in jobargs.items() if (v is not None) and (k in self.JOB_SPEC))
                jid=jobargs.get('id',str(uuid.uuid1())) #use preset id or generate one
                job=self.__jobs.get(jid)
                if job: #modifying an existing job
                    del jobargs['id'] #unset incoming id
                    del job['ts'] #unset ts to ensure update 
                    #do sanity checks on state changes
                    #inactive jobs can only restarted
                    if job['state'] in self.JOB_INACTIVE:
                        if 'state' in jobargs:
                            if jobargs['state']=='new': 
                                #if no node specified, routing logic will set one
                                if 'node' not in jobargs: jobargs['node']=False
                            else: del jobargs['state'] #not allowed
                    #active jobs can only be killed, and cannot be moved
                    else:
                        if 'state' in jobargs and jobargs['state'] != 'killed':
                            del jobargs['state']
                        if 'node' in jobargs: del jobargs['node']
                        if 'pool' in jobargs: del jobargs['pool']
                else: #this is a new job
                    if not jobargs.get('pool'): return False #jobs have to have a pool to run in
                    job={  
                            'submit_ts':time.time(),    #submit timestamp
                            'node':False,               #no node assigned unless jobargs set one
                            'state':'new',
                            'start_count':0,             
                            'fail_count':0
                        }
                job.update(**jobargs)
                #if not job.get('node'): job['node']=self.node #set job to be handled/routed by this node
                self.__update_job(jid,**job)
                return jid
            except Exception as e: self.logger.warning(e,exc_info=True)

    def __state_run(self):
        self.logger.info('started')
        checkpoint_count=0
        while not self.shutdown.is_set():
            self.logger.debug('status %s'%self.__node_status)
            self.logger.debug('pools %s'%self.__pool_status)
            with self.__lock:
                try:
                    #write recently finished jobs to history
                    for jid,job in self.__jobs.items():
                        if job['seq'] > self.__hist_seq and job['state'] in self.JOB_INACTIVE:
                            self.write_history(jid)
                    self.__hist_seq=self.__seq
                    #scan for jobs that need a node
                    for jid,job in self.__jobs.items():
                        if job['state'] == 'new' and time.time()-job['ts'] > self.timeout:
                            self.logger.debug('job %s still in state new'%jid)
                            self.__update_job(jid) #touch the job to resend it downstream
                    #scan for expired jobs
                    for jid,job in self.__jobs.copy().items():
                        if time.time()-job['ts'] > self.expire: 
                            #jobs that have ended will no longer be updated, so expire them.
                            if job['state'] in self.JOB_INACTIVE:
                                self.logger.debug('expiring inactive job %s'%jid)
                                del self.__jobs[jid]
                            #if we expire active jobs
                            elif self.expire_active_jobs and job.get('active'): 
                                #this job *should* have been updated
                                self.logger.warning('active job %s not updated in %s seconds'%(jid,self.expire))
                                #if we restart on fail, reset the job
                                if job.get('retries'): self.__update_job(jid,active=False,node=False) 
                                #set job to failed, it will restart if it can
                                self.__update_job(jid,state='failed',error='expired')
                    #set nodes that have not sent status to offline
                    for node,node_status in self.__node_status.copy().items():
                        if time.time()-node_status.get('ts',0) > self.timeout:
                            if node_status.get('online'):
                                self.logger.warning('node %s not updated in %s seconds'%(node,self.expire))
                                self.__update_node_status(node,online=False)
                            elif node_status.get('remove'): #offline node is marked for upstream removal
                                self.logger.info('removing node %s' %node)
                                del self.__node_status[node]
                    #remove offline nodes from pool status and remove empty pools
                    if time.time()-self.__pool_ts > self.timeout:
                        for pool,nodes in self.__pool_status.copy().items():
                            for node,slots in nodes.copy().items():
                                if slots is False: 
                                    self.logger.info('removing node %s from pool %s'%(node,pool))
                                    del self.__pool_status[pool][node]
                            if not self.__pool_status[pool]:
                                self.logger.info('removing empty pool %s'%(pool))
                                del self.__pool_status[pool]
                        self.__pool_ts=time.time()
                except Exception as e: self.logger.warning(e,exc_info=True)
                if self.checkpoint:
                    checkpoint_count=(checkpoint_count+1) % self.checkpoint
                    if not checkpoint_count: self.__save_state()
            time.sleep(1)

        #close history file if we have one
        if self.hist_fh: self.hist_fh.close()

        #save state at shutdown
        with self.__lock: self.__save_state()