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
            nodelist: [optional] list of nodes to prefer, to set downstream routing
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
            state: the job state (new,waiting,running,done,failed,killed)
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
                'nodelist',
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

    #states of active jobs
    JOB_ACTIVE=['waiting','running']

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
        self.config(**cfg)
        self.start()

    def config(self,expire=60,expire_active_jobs=True,history=None,**cfg):
        with self.__lock:
            if expire: self.expire=int(expire)
            self.expire_active_jobs=expire_active_jobs
            if history:
                try: self.hist_fh=open(history,'a')
                except Exception as e: self.logger.warning('%s:%s'%(history,e))
            elif self.hist_fh: 
                self.hist_fh.close()
                self.hist_fh=None

    def write_history(self,jid):
        if self.__jobs[jid]['state'] in self.JOB_INACTIVE and self.hist_fh:
            json.dump({jid:self.__jobs[jid]},self.hist_fh)
            self.hist_fh.write('\n')
            self.hist_fh.flush()

    #these return a copy of the private state, use update_ methods to modify it
    def get_pool_status(self): 
        '''get a pool:node:slots_free map of pool availability'''
        return self.__pool_status.copy()

    def get_node_status(self): 
        '''get a node:status map of cluster status'''
        return self.__node_status.copy()
    
    def update_pool_status(self,pool,node,slots): 
        '''set slots on node in pool'''
        with self.__lock: self.__update_pool_status(pool,node,slots)
    def __update_pool_status(self,pool,node,slots):
        self.__pool_status.setdefault(pool,{})[node]=slots
        if slots is False: self.__pool_ts=time.time() #reset expiration on remove

    def update_node_status(self,node,**node_status): 
        '''set node status, remove node from pool if node offline'''
        with self.__lock:
            self.__update_node_status(node,**node_status)
    def __update_node_status(self,node,**node_status): 
        self.__node_status.setdefault(node,{}).update(**node_status)
        #take offline nodes out of the pools
        if not node_status.get('online'):
            for pool in self.__pool_status.keys():
                if node in self.__pool_status[pool] \
                    and self.__pool_status[pool][node] is not False:
                        self.__update_pool_status(pool,node,False)

    def get(self,id=None,ts=None,seq=None,**query):
        '''dump all jobs for a node/pool/state/or updated after a certain ts/seq'''
        with self.__lock:
            try: #filter by ts/seq greater than
                r=dict((jid,job.copy()) for (jid,job) in self.__jobs.items() if \
                     (not ts or job['ts']>ts) and (not seq or job['seq']>seq) )
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
                        if not job.get('node'): job['node']=self.node #may not be set from client
                        self.__jobs.setdefault(jid,{}).update(job,seq=self.__seq)
                        self.__seq+=1
                        updated.append(jid)
                #update our status from incoming status data
                for node,node_status in status.get('nodes',{}).items():
                    #for the node we are syncing from
                    #save the list of nodes it has seen
                    if remote_node and node==remote_node: 
                        node_status['routing']=list(status['nodes'].keys())
                    self.__update_node_status(node,**node_status)
                for pool,nodes in status.get('pools',{}).items():
                    for node,slots in nodes.items(): 
                        self.__update_pool_status(pool,node,slots)
            except Exception as e: self.logger.warning(e,exc_info=True)
        #if updated job is finished, write it to history
        for jid in updated: self.write_history(jid)
        #return updated items
        return updated

    def get_job(self,jid):
        '''return job jid's data from state'''
        with self.__lock:
            try:
                if jid in self.__jobs: return self.__jobs.get(jid).copy()
            except Exception as e: self.logger.warning(e,exc_info=True)
    
    def update_job(self,jid,**data):
        '''update job jid with k/v in data
        this is ONLY to be used by the agent as no sanity checks are performed'''
        with self.__lock:
            try:
                if jid in self.__jobs: 
                    self.__jobs[jid].update(seq=self.__seq,ts=time.time(),**data)
                    self.__seq+=1
                    #if done, write job to history
                    self.write_history(jid)
                    return self.__jobs.get(jid)
                else: return False
            except Exception as e: self.logger.warning(e,exc_info=True)

    def kill_jobs(self,*args,**kwargs):
        '''kill jobs, args can be a job id, a list of jobids, or a query dict'''
        resp={}
        if kwargs: arg=kwargs
        elif args: arg=args[0]
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
                    del jobargs['id']
                    #do sanity checks on state changes
                    #inactive jobs can only restarted
                    if job['state'] in self.JOB_INACTIVE:
                        if 'state' in jobargs and jobargs['state']!='new': del jobargs['state']
                    #other jobs can only killed, and active jobs cannot be moved to another pool/node
                    else:
                        if 'state' in jobargs and jobargs['state']!='killed': del jobargs['state']
                        if job['state'] in self.JOB_ACTIVE:
                            if 'node' in jobargs: del jobargs['node']
                            if 'pool' in jobargs: del jobargs['pool']
                else: #this is a new job
                    if not jobargs.get('args') or not jobargs.get('pool'): return False #jobs have to have a command and pool to run in
                    job={  
                            'submit_ts':time.time(),    #submit timestamp
                            'nodelist':[],              #list of nodes to handle this job
                            'state':'new',              #job state
                            'start_count':0,             
                            'fail_count':0
                        }
                job.update(seq=self.__seq,ts=time.time(),**jobargs)
                if not job.get('node'): job['node']=self.node #set job to be handled/routed by this node
                self.__jobs[jid]=job
                #if done, write job to history
                self.write_history(jid)
                self.__seq+=1
                return jid
            except Exception as e: self.logger.warning(e,exc_info=True)

    def __state_run(self):
        self.logger.info('started')
        while not self.shutdown.is_set():
            self.logger.debug('status %s'%self.__node_status)
            self.logger.debug('pools %s'%self.__pool_status)
            with self.__lock:
                try:
                    #look for jobs that should have been updated
                    for jid,job in self.__jobs.copy().items():
                        if time.time()-job['ts'] > self.expire: 
                            #jobs that have ended for some reason will no longer be updated, so expire them.
                            if job['state'] in self.JOB_INACTIVE:
                                self.logger.debug('expiring job %s'%jid)
                                del self.__jobs[jid]
                            elif self.expire_active_jobs: 
                                #this job *should* have been updated
                                self.logger.warning('job %s not updated in %s seconds'%(jid,self.expire))
                                #if we restart on fail and have a nodelist
                                if job.get('retries') and job['nodelist']:
                                    # try kicking it back to the first node for rescheduling
                                    self.__jobs[jid].update(node=job['nodelist'][0]) 
                                #set job to failed, it might restart if it can
                                self.__jobs[jid].update(seq=self.__seq,ts=time.time(),state='failed',error='expired')
                                self.write_history(jid)
                                self.__seq+=1
                    #set nodes that have not sent status to offline
                    for node,node_status in self.__node_status.copy().items():
                        if time.time()-node_status.get('ts',0) > self.expire:
                            if node_status.get('online'):
                                self.logger.warning('node %s not updated in %s seconds'%(node,self.expire))
                                self.__update_node_status(node,online=False)
                            elif node_status.get('remove'): #offline node is marked for upstream removal
                                self.logger.info('expiring node %s' %node)
                                del self.__node_status[node]
                    #expire removed nodes from pool status and remove empty pools
                    if time.time()-self.__pool_ts > self.expire:
                        for pool,nodes in self.__pool_status.copy().items():
                            for node,slots in nodes.copy().items():
                                if slots is False: 
                                    self.logger.info('expiring node %s from pool %s'%(node,pool))
                                    del self.__pool_status[pool][node]
                            if not self.__pool_status[pool]:
                                self.logger.info('expiring empty pool %s'%(pool))
                                del self.__pool_status[pool]
                        self.__pool_ts=time.time()
                except Exception as e: self.logger.warning(e,exc_info=True)
            time.sleep(1)

        #close history file if we have one
        if self.hist_fh: self.hist_fh.close()