#1/usr/bin/env python3

import time
import threading
import logging

from .state import State
from .node import Node
from .util import cmdline_parser

'''
Provides request-based and job-based APIs
Examples:

from meeseeks import Client
c=Client('localhost')
c.get_nodes() #get cluster status
c.get_pools() #get pools
c.get()             #get all jobs
jid=c.submit_job(pool=...,args=[.....])  #submit a job, return the ID
c.get_job(j)        #check the job
c.kill_jobs(j)       #stop the job(s)
c.close()           #disconnect client


from meeseeks import Job
j=Job(cmd,args...) #create Job object, auto-connect to cluster
j.pool='p1' #set the pool
j.start() #submit the job
j.state #check the job state
j.start_ts #check when it started
j.restart=True #set job to restart on exit
j.poll() #check if it finished or failed
j.kill() #kill the job
'''


class Client(State):
    '''client class to connect to a node and manage the state of it and all downstream nodes.
        Client methods are available to make direct requests
        State object's methods are available to get status and manage jobs
        to use State methods, set refresh > 0 to start node sync thread
    '''
    def __init__(self,address=None,port=13700,timeout=10,refresh=0,poll=10,expire=60,set_global=False,**cfg):

        #state object to cache cluster state from the node
        State.__init__(self,None,
                        expire=expire,
                        expire_active_jobs=False)
     
        #internal node object to communicate with the node
        #refresh defaults to 0 for client, set > 0 to start node sync thread
        self.__node=Node(None,None,self,
                        address=address,
                        port=port,
                        timeout=timeout,
                        refresh=refresh,
                        poll=poll,
                        **cfg)

        #set the global client
        global _CLIENT
        if set_global: _CLIENT=self


    #direct request methods
    
    # submit new job or changes to existing job
    # if existing job is specified with id=... job will be modified
    def submit(self,**kwargs): return self.__node.request([{'submit':kwargs}])[0]['submit']

    #query/kill jobs
    # specify single jid to get single job dict back
    # specify kwargs to filter on key=value
    # special keys are: 
    #  ids=[list of job ids], 
    #  ts=(returns jobs with ts >= ts), 
    #  seq=(returns jobs with seq >= seq)
    def query(self,jid=None,**kwargs): 
        if jid: return self.__node.request([{'job':jid}])[0]['job']
        else: return self.__node.request([{'get':kwargs}])[0]['get']
    def kill(self,jid=None,**kwargs): 
        if jid: return self.__node.request([{'kill':jid}])[0]['kill']
        else: return self.__node.request([{'kill':kwargs}])[0]['kill']

    #return list of job ids for jobs matching kwargs criteria
    def ls(self,**kwargs): return self.__node.request([{'ls':kwargs}])[0]['ls']
    
    #get node and pool status, kwargs are sent but ignored (for now)
    def nodes(self,**kwargs): return self.__node.request([{'nodes':kwargs}])[0]['nodes']
    def pools(self,**kwargs): return self.__node.request([{'pools':kwargs}])[0]['pools']
    
    #for sending raw requests
    def request(self,req): return self.__node.request([req])[0]
    
    def close(self): 
        #stop the node and state threads
        self.__node.shutdown.set()
        self.__node.join()
        self.shutdown.set()
        self.join()


#global client object for Job
global _CLIENT
_CLIENT=None
#default global client conf
_CLIENT_CONF=dict( refresh=10 )

class Job():
    '''Job-based API to Meeseeks
    job info is be available as Job attributes. ex: Job.state, Job.rc
    these attributes at kept in sync with the actual jobs
    Job.info can be used to read the cached job without refreshing'''

    JOB_INACTIVE=State.JOB_INACTIVE
    JOB_INACTIVE.append(False) #special case for us, if a job no longer exists

    def __init__(self,*args,**kwargs):
        '''create a job spec and a client if not already connected
    if no Client object is passed in the client= argument, use the global client
    the global client is configured using the MEESEEKS_CONF environment variable

    the Job instance is passed the job spec as keyword args, and treats any positional args as job args
    example: job=Job('sleep','1000',pool='p1')

    
    '''
        #set attrubutes
        #we do this via the __dict__ reference to bypass self.__setattr__
        self.__dict__['jid']=None #job id or list of job IDs from submit
        self.__dict__['multi']=False #multinode job, job atttributes will be id:value mappings vs

        #create/use the global client if we don't have one
        global _CLIENT,_CLIENT_CONF
        client=kwargs.get('client')
        if client: self.__dict__['client']=client
        else:
            if not _CLIENT: _CLIENT=Client(**_CLIENT_CONF)
            self.__dict__['client']=_CLIENT
        while not self.client.get_nodes(): time.sleep(1)

        #set the self.info attrubute, this will be the cache of job state
        if args: kwargs.update(args=list(args))
        self.__dict__['info']=dict((k,v) for (k,v) in kwargs.items() if k in State.JOB_SPEC)

        #detect multi-submit job
        if 'node' in self.info and \
            (type(self.info.get('node')) is list or self.info['node'].endswith('*')):
                self.__dict__['multi']=True

    def __getattr__(self,attr=None):
        '''refresh the job(s) and return the attribute'''
        if not self.jid: return False
        jobs=self.client.get(self.jid)
        if self.multi:
            for jid in self.jid:
                if jid in jobs: self.info.setdefault(jid,{}).update(jobs[jid])
                else: self.info.setdefault(jid,{}).update(state=False) #job expired before we checked it 
            if attr is not None: return dict((jid,job.get(attr)) for (jid,job) in self.info.items())
        elif jid in jobs: self.info.update(jobs[jid])
        else: self.info.update(state=False)
        if attr is not None: return self.info.get(attr)

    def __setattr__(self,attr,value):
        '''set attr=value in the job(s) and submit to the client'''
        self.__getattr__() #sync cache first to make sure job exists
        if self.multi:
            if self.jid:
                for jid in self.jid:
                    if self.info[jid]['state']:
                        self.info[jid][attr]=value
                        self.info[jid]['id']=jid #set id to modify existing job
                        self.client.submit_job(**self.info[jid]) #submit modified job
        elif self.info['state']:
            self.info[attr]=value
            self.info['id']=self.jid
            self.client.submit_job(**self.info)
        self.__getattr__() #sync cache again
        
    def start(self):
        '''start the job(s) by submitting it to the client
        returns job id/job ids from submit'''
        if self.jid: return False #job already started
        r=list(self.client.submit_job(**self.info).keys())
        if self.multi:
            self.__dict__['info']={} #clear cache to remove submit data
            self.__dict__['jid']=r
        else: self.__dict__['jid']=r[0]
        self.__getattr__() #sync cache
        return self.jid
    
    def kill(self):
        '''stop running job(s)'''
        self.client.kill_jobs(self.jid)
        self.__getattr__() #refresh cache

    def is_alive(self):
        '''returns True if job (or if multi, any jobs) have not finished'''
        if self.multi: return any((state not in self.JOB_INACTIVE) for (jid,state) in self.state.items())
        else: return self.active

    def poll(self):
        '''returns info if a job finished, None if running
        if multi, returns finished jobs or empty dict if none'''
        if self.multi: 
            self.__getattr__() #refresh cache
            return dict((jid,job) for (jid,job) in self.info.items() if (job['state'] in self.JOB_INACTIVE))
        else:
            if self.state in self.JOB_INACTIVE: return None #refresh and get active flag
            else: return self.info








