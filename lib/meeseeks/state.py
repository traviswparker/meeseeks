#!/usr/bin/env python3

import os
import time
import threading
import logging
import uuid
import json

class State(threading.Thread):
    '''cluster state interface
        job submit spec is:
            id: [optional] manually set id of the job, a UUID will be generated if not specified
            uid: [optional] userid of the job
            gid: [optional] groupid of the job
            pool: <required> the pool the job runs in
            args: <required> list of [ arg0 (executable) [,arg1,arg2,...] ] (command line for job)
            env: [optional] environment for job, usually set to dict(os.environ) by client
            node: [optional] node to run on, job will fail if unavailable
            filter: [optional] pattern to match nodename against, to set preferred nodes
            stdin: filename to redirect stdin from
            stdout: filename to redirect stdout to
            stderr: filename to redirect stderr to
            restart: if true, job will be restarted when done
            retries: count of times to restart a failed job
            resubmit: if true, when job is restarted/retried, send back to the submit_node vs. restarting locally 
            runtime: job maximum runtime in seconds. job will be killed and marked as failed if exceeded.
            hold: if true, job will not run until cleared
            config: job task configuration dict. for Task spawned by Pool, sets popen args.
            tags: list of tags, can be matched in query with tag=

        job attributes:
            node: the node the job is assigned to
            submit_node: the node the job was submitted to
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
                'uid',
                'gid',
                'pool',
                'args',
                'env',
                'state',
                'node',
                'filter',
                'stdin',
                'stderr',
                'stdout',
                'restart',
                'retries',
                'resubmit',
                'runtime',
                'hold',
                'config',
                'tags'
            ]

    #states of inactive jobs
    JOB_INACTIVE=['done','failed','killed']

    def __init__(self,node=None,**cfg):
        self.node=node
        name='State'
        if self.node: name=self.node+'.'+name
        threading.Thread.__init__(self,daemon=True,name=name,target=self.__state_run)
        self.logger=logging.getLogger(self.name)
        self.shutdown=threading.Event()
        self.__lock=threading.Lock() #lock on __jobs dict
        self.__jobs={} #(partial) cluster job state, this is private because we lock during any changes
        self.__status={} #map of node:{online:bool, routing:[nodes seen], pools:{pool:slots} }
        self.__seq=1 #update sequence number. Always increments.
        self.hist_fh=None
        self.__hist_seq=0 #history sequence number.
        self.state_file=None
        self.checkpoint=None
        self.config(**cfg)
        self.__load_state()
        self.start()

    def config(self,expire=300,expire_active_jobs=True,timeout=60,history=None,file=None,checkpoint=None,**cfg):
        with self.__lock:
            if expire: self.expire=int(expire)
            if timeout: self.timeout=int(timeout)
            self.expire_active_jobs=expire_active_jobs
            if file: self.state_file=file
            if checkpoint is not None: self.checkpoint=checkpoint
            if history:
                try: self.hist_fh=open(history,'a')
                except Exception as e: self.logger.warning('%s:%s',history,e)
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
                    self.logger.info('saved state to %s',self.state_file)
            except Exception as e: self.logger.warning('%s:%s',self.state_file,e)

    def __load_state(self):
        if self.state_file:
            try:
                with open(self.state_file) as fh: 
                    self.__jobs=json.load(fh)
                    self.logger.info('loaded state from %s',self.state_file)
            except Exception as e: self.logger.warning('%s:%s',self.state_file,e)

    #these return a copy of the state, use update_ methods to modify it

    def get_nodes(self): 
        '''get a node:status map of cluster status'''
        return self.__status.copy()
    def update_node(self,node,**node_status): 
        '''set node status, remove node pools if node offline'''
        with self.__lock: self.__update_node(node,**node_status)
    def __update_node(self,node,**node_status): 
        self.__status.setdefault(node,{'pools':{}}).update(**node_status)
        #remove offline nodes from pools
        if not self.__status[node].get('online'): self.__status[node]['pools']={}

    def get_pools(self): 
        '''get a pool:node:slots_free map of pool availability'''
        with self.__lock: return self.__get_pools()
    def __get_pools(self):
        pools={}
        for n,node in self.get_nodes().items():
            for pool,slots in node['pools'].items():
                if slots is not True: #if limit set, subtract pending/running jobs
                    slots-=len( [ job for job in self.__get(node=n,pool=pool).values() \
                        if job['state'] not in self.JOB_INACTIVE ] )
                pools.setdefault(pool,{})[n]=slots
        return pools
    def update_pool(self,pool,node,slots): 
        '''set slots in pool for node'''
        with self.__lock: self.__update_pool(pool,node,slots)
    def __update_pool(self,pool,node,slots):
        if slots: self.__status.setdefault(node,{'pools':{}})['pools'][pool]=slots
        elif node in self.__status and pool in self.__status[node]['pools']: 
            del self.__status[node]['pools'][pool]

    def get(self,ids=[],ts=None,seq=None,**query):
        '''dump a list of jobs or all jobs for a node/pool/state/or updated after a certain ts/seq'''
        with self.__lock: return self.__get(ids,ts,seq,**query)
    def __get(self,ids=[],ts=None,seq=None,tag=None,**query):
        try: 
            #turn single job id into list
            if ids and type(ids) is not list: ids=[ids]
            #filter by jid list and/or ts/seq greater than
            #filter by tag in tags if specified
            #if we're on a node, do not return jobs without node unless seq/ts/node specified
            # (prevents propagation of unrouted jobs)
            r=dict( (jid,job.copy()) for (jid,job) in self.__jobs.items() if \
                    (jid in ids) or ( not ids \
                        and (not ts or job['ts']>ts) \
                        and (not seq or job['seq']>seq) \
                        and (not tag or tag in job['tags']) \
                        and (not self.node or job['node'] or 'node' in query) ) )
            for (k,v) in query.items(): #filter by other criteria
                if type(v) is str and v.endswith('*'): #wildcard on string attrs
                    r=dict((jid,job) for (jid,job) in r.items() if \
                        type(job.get(k)) is str and job.get(k).startswith(v[:-1]))
                else: r=dict((jid,job) for (jid,job) in r.items() if job.get(k)==v)
            return r
        except Exception as e: self.logger.warning(e,exc_info=True)
        return None

    def sync(self,jobs={},status={}):
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
                for node,node_status in status.items():
                    self.__update_node(node,**node_status)
            except Exception as e: self.logger.warning(e,exc_info=True)
        #return updated items
        return updated

    def get_job(self,jid):
        '''return job jid's data from state'''
        try:
            if jid in self.__jobs: return self.__jobs.get(jid).copy()
        except Exception as e: self.logger.warning(e,exc_info=True)

    def update_job(self,jid,**data):
        '''update job jid with k/v in data, no sanity checks are performed'''
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
        r={} #returned jid:job info map
        with self.__lock:
            try:
                #filter job to spec keys
                jobargs=dict((k,v) for (k,v) in jobargs.items() if (v is not None) and (k in self.JOB_SPEC))

                #handle multi-node spec
                if jobargs.get('node'):
                    nodes=jobargs['node']
                    del jobargs['node']
                    if type(nodes) is not list: #if nodes is already a list of nodenames, use it
                        if nodes.endswith("*"): #wildcard specified
                            #get all nodes in the pool matching the pattern. 
                            nodes=[ node for node in \
                                    self.__get_pools().get(jobargs['pool'],{}).keys() \
                                    if node.startswith(nodes[:-1]) ]
                        else: nodes=[nodes] #single node specifies
                else: nodes=[None] #nothing specified

                #create a job for each node
                for node in nodes:
                    if node: jobargs['node']=node
                    jid=jobargs.get('id',str(uuid.uuid1())) #use preset id or generate one
                    job=self.__jobs.get(jid)
                    if job: #modifying an existing job
                        del jobargs['id'] #unset incoming id
                        del job['ts'] #unset ts to ensure update 
                        #do sanity checks on state changes
                        #inactive jobs can only reset
                        if job['state'] in self.JOB_INACTIVE:
                            if 'state' in jobargs:
                                if jobargs['state']=='new': 
                                    #if no node specified, routing logic will set one
                                    if jobargs.get('node'): jobargs['submit_node']=jobargs['node'] #change submit node if set
                                    else: 
                                        jobargs.update( 
                                            node=job.get('submit_node',False), #reset to submit node if set
                                            active=False, # clear active state if removed from node
                                            error=None, #clear error state
                                            start_count=0,fail_count=0, #reset counts
                                            submit_ts=time.time() #reset submit ts
                                        )
                                else: del jobargs['state'] #other state change not allowed
                        #active jobs can only be killed, and cannot be moved
                        else:
                            if 'state' in jobargs and jobargs['state'] != 'killed': del jobargs['state']
                            if 'node' in jobargs: del jobargs['node']
                            if 'pool' in jobargs: del jobargs['pool']
                    else: #this is a new job
                        if not jobargs.get('pool'): return {jid:False} #jobs have to have a pool to run in
                        if 'state' in jobargs: del jobargs['state'] #new jobs can't have a state
                        #tags must be a list
                        if type(jobargs.get('tags')) is not list: jobargs['tags']=[jobargs.get('tags')]
                        job={  
                                'submit_ts':time.time(),    #submit timestamp
                                'node':jobargs.get('node',False),               #no node assigned unless jobargs set one
                                'submit_node':jobargs.get('node',False),        #node job was submitted to, we reset to this
                                'state':'new',
                                'start_count':0,             
                                'fail_count':0,
                                'error':None,
                                'active':False,
                                'uid':os.geteuid()
                            }
                    job.update(**jobargs)
                    self.__update_job(jid,**job)
                    self.logger.info('submit job %s',jid)
                    r[jid]=job.copy()

            except Exception as e: self.logger.warning(e,exc_info=True)
            return r

    def __state_run(self):
        self.logger.info('started')
        checkpoint_count=0

        while not self.shutdown.is_set():
            with self.__lock:
                try:
                    #snapshot node status so we can modify it
                    nodes=self.__status.copy()

                    #write recently finished jobs to history
                    for jid,job in self.__jobs.items():
                        if job['seq'] > self.__hist_seq and job['state'] in self.JOB_INACTIVE:
                            self.write_history(jid)
                    self.__hist_seq=self.__seq

                    #scan for expired jobs
                    for jid,job in self.__jobs.copy().items():
                        if time.time()-job['ts'] > self.expire: 
                            #jobs that have ended will no longer be updated, so expire them.
                            if job['state'] in self.JOB_INACTIVE:
                                self.logger.debug('expiring inactive job %s',jid)
                                del self.__jobs[jid]
                            #if we expire active jobs and the job's node is down
                            elif self.expire_active_jobs and not nodes.get(job.get('node'),{}).get('online'):
                                #this job *should* have been updated by the node
                                self.logger.warning('expiring active job %s on %s',jid,job.get('node'))
                                #set job to failed
                                self.__update_job(jid,state='failed',error='expired',fail_count=job.get('fail_count',0)+1)
                            #bump the timestamp on the job to ensure it has been forwarded to the proper node
                            else: self.__update_job(jid)
                            
                    #scan for recurring jobs
                    for jid,job in self.__jobs.items():
                        #resubmit kicks done/fail jobs back to the submit node for pool reassignment
                        #is this our job (assigned to us and not set to resubmit)
                        this_node=( self.node and job.get('node')==self.node ) and not job.get('resubmit') #job on this node
                        #resubmittable and not claimed 
                        #and not new or killed 
                        #and we are the node that will resubmit it
                        resubmit=(  job.get('resubmit') and not job.get('active') \
                                    and job['state'] not in ['new','killed'] \
                                    and (self.node and job.get('submit_node') == self.node) )
                        if resubmit or this_node: #only check jobs we may be restarting/resubmitting
                            restart=False
                            #if we restart and this job is done
                            if job.get('restart') and job['state'] == 'done': restart=True
                            #if we retry on fail and we have retries remaining
                            elif job['state'] == 'failed' and job.get('fail_count') <= job.get('retries',0):
                                self.logger.info('retry job %s (%s of %s)',jid,job.get('fail_count'),job.get('retries'))
                                restart=True
                            if restart: 
                                if resubmit: #claim this job to resubmit it 
                                    self.logger.info('resubmit job %s',jid)
                                    self.__update_job(jid, submit_ts=time.time(), state='new', node=self.node)
                                else: #restart locally 
                                    self.logger.info('restart job %s',jid)
                                    self.__update_job(jid, state='new')

                    #set nodes that have not sent status to offline
                    for node,node_status in nodes.items():
                        if time.time()-node_status.get('ts',0) > self.timeout:
                            if node_status.get('online'):
                                self.logger.warning('node %s not updated in %s seconds',node,self.timeout)
                                self.__update_node(node,online=False)
                            elif node_status.get('remove'): #offline node is marked for upstream removal
                                self.logger.info('removing node %s',node)
                                del self.__status[node]

                except Exception as e: self.logger.warning(e,exc_info=True)

                #save to state file if checkpointing set
                if self.checkpoint:
                    checkpoint_count=(checkpoint_count+1) % self.checkpoint
                    if not checkpoint_count: self.__save_state()

            time.sleep(1)

        #close history file if we have one
        if self.hist_fh: self.hist_fh.close()

        #save state at shutdown
        with self.__lock: self.__save_state()
