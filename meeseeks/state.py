#!/usr/bin/env python3

import time
import threading
import logging

class State(threading.Thread):
    '''cluster state handler'''
    def __init__(self,node,refresh=10,expire=300):
        self.node=node
        threading.Thread.__init__(self,daemon=True,name=self.node+'.State',target=self.state_run)
        self.logger=logging.getLogger(self.name)

        self.shutdown=threading.Event()

        self.refresh=refresh
        self.expire=expire
    
        '''state is:
            __jobs: { jid: {pool: node: ts: state: [jobargs...] }} }
            jid: uuid of the job
            ts: the last updated timestamp of the job
            pool: the pool (queue) the job runs in 
            node: the node the job is assigned to
            state: the job state
        '''
        self.__lock=threading.Lock() #lock on __jobs dict
        self.__jobs={} #(partial) cluster job state, this is private because we lock during any changes
        self.pool_status={} #map of [pool][node][open slots] for nodes we connect downstream to
        self.node_status={} #map of node:last status

        self.start()

    def set_node_status(self,node,**kwargs): self.node_status.setdefault(node,{}).update(**kwargs)
    def set_pool_status(self,pool,node,slots): self.pool_status.setdefault(pool,{})[node]=slots

    def get(self,node=None,pool=None,ts=None):
        #dump all state for a node/pool/or updated after a certain ts
        with self.__lock:
            try: 
                return dict((jid,job.copy()) for (jid,job) in self.__jobs.items() if \
                    (   (not node or job['node']==node) and \
                        (not pool or job['pool']==pool) and \
                        (not ts or job['ts']>ts) )
                )
            except Exception as e: self.logger.warning(e,exc_info=True)
        return None

    def sync(self,jobs={},status={}):
        #update local state with incoming state if job not in local state or job ts >= local jobs ts
        with self.__lock:
            updated=[]
            try:
                for jid,job in jobs.items():
                    if jid not in self.__jobs or self.__jobs[jid]['ts'] <= job['ts']: 
                        self.__jobs.setdefault(jid,{}).update(job)
                        updated.append(jid)
            except Exception as e: self.logger.warning(e,exc_info=True)
        #update node status and pool availabiliy
        try:
            for node,node_status in status.get('nodes',{}).items():
                self.set_node_status(node,**node_status)
            for pool,nodes in status.get('pools',{}).items():
                for node,slots in nodes.items(): self.set_pool_status(pool,node,slots)
        except Exception as e: self.logger.warning(e,exc_info=True)
        #return updated items
        return updated

    def get_job(self,jid):
        #get job by ID
        with self.__lock:
            try:
                if jid in self.__jobs: return self.__jobs.get(jid).copy()
            except Exception as e: self.logger.warning(e,exc_info=True)
    
    def update_job(self,jid,**data):
        #get job by ID
        with self.__lock:
            try:
                if jid in self.__jobs: 
                    self.__jobs[jid].update(ts=time.time(),**data)
                    return self.__jobs.get(jid)
                else: return False
            except Exception as e: self.logger.warning(e,exc_info=True)

    def add_job(self,**jobargs):
        #add a new job to the state
        with self.__lock:
            try:
                if 'pool' not in jobargs or 'cmd' not in jobargs: return False #jobs have to have a command and pool to run in
                jid=jobargs.get('id',str(uuid.uuid1())) #use preset id or generate one
                if jid in self.__jobs: #job exists
                    self.logger.warning('add_job: %s exists'%jid)
                    return False
                self.__jobs[jid]={  'ts':time.time(),           #last updated timestamp
                                    'submit_ts':time.time(),    #submit timestamp
                                    'start_ts':None,            #job start
                                    'end_ts':None,              #job end
                                    'pool':None,                #pool to run in 
                                    'nodelist':[],              #list of nodes allowed to handle this job
                                    'node':self.node,           #node job is currently on
                                    'state':'new'               #job state
                                }             
                self.__jobs[jid].update(jobargs)
                return jid
            except Exception as e: self.logger.warning(e,exc_info=True)

    def state_run(self):
        self.logger.info('started')
        while not self.shutdown.is_set():
            self.logger.debug(self.node_status)
            self.logger.debug(self.pool_status)
            #look for jobs that should have been updated
            with self.__lock:
                try:
                    for jid,job in self.__jobs.copy().items():
                        if time.time()-job['ts'] > self.expire: 
                            #jobs that have ended for some reason will no longer be updated, so expire them.
                            if job['state'] in ['done','killed','failed']:
                                self.logger.debug('expiring job %s'%jid)
                                del self.__jobs[jid]
                            else: 
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
            for node,node_status in self.node_status.items():
                if node_status.get('online') and time.time()-node_status['ts'] > self.expire:
                    self.logger.warning('node %s not updated in %s seconds'%(node,self.expire))
                    node_status.update(online=False)
            time.sleep(self.refresh)
