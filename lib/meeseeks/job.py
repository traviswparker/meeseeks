#!/usr/bin/env python3
 
import os
import time
import threading
import logging

from .state import State
from .client import Client
from .util import cmdline_parser

global _CLIENT, _CLIENT_CONF, _NOTIFY
#default global client conf
_CLIENT_CONF=dict(refresh=1)
_CLIENT_CONF.update(cmdline_parser(os.getenv('MEESEEKS_CONF','').split())[0])
_CLIENT=_NOTIFY=None

class Job():
    '''Job-based API to Meeseeks
    job info is be available as Job attributes. ex: Job.state, Job.rc
    these attributes at kept in sync with the actual jobs
    Job.info can be used to read the cached job without refreshing'''

    def __init__(self,*args,**kwargs):
        '''create a job spec and a client if not already connected
    if no Client object is passed in the client= argument, use the global client
    the global client is configured using the MEESEEKS_CONF environment variable

    the Job instance is passed the job spec as keyword args, and treats any positional args as job args
    example: job=Job('sleep','1000',pool='p1')

    Tracked jobs will call the function set with arg notify=<function> when the job (or any subjobs) exit
    '''
        #set attrubutes
        self.jid=None #job id or list of job IDs from submit
        self.multi=False #multinode job, job atttributes will be id:value mappings vs

        #create/use the global client if we don't have one
        global _CLIENT,_CLIENT_CONF
        client=kwargs.get('client')
        if client: self.client=client
        else:
            if not _CLIENT: _CLIENT=Client(**_CLIENT_CONF)
            self.client=_CLIENT
        while not self.client.get_nodes(): time.sleep(1)
        self.notify=kwargs.get('notify')

        #set the self.info attrubute, this will be the cache of job state
        if args: kwargs.update(args=list(args))
        self.info=dict((k,v) for (k,v) in kwargs.items() if k in State.JOB_SPEC)

        #detect multi-submit job
        if 'node' in self.info and \
            (type(self.info.get('node')) is list or self.info['node'].endswith('*')):
                self.multi=True

    def __getattr__(self,attr=None):
        '''refresh the job(s) and return the attribute'''
        if not self.jid: return False
        jobs=self.client.get(self.jid)
        if self.multi:
            for jid in self.jid:
                if jid in jobs: self.info.setdefault(jid,{}).update(jobs[jid])
                else: self.info.setdefault(jid,{}).update(state=False) #job expired before we checked it 
            if attr is not None: return dict((jid,job.get(attr)) for (jid,job) in self.info.items())
        elif self.jid in jobs: self.info.update(jobs[self.jid])
        else: self.info.update(state=False)
        if attr is not None: return self.info.get(attr)

    def __setattr__(self,attr,value):
        '''set attr=value in the job(s) and submit to the client'''
        if attr in State.JOB_SPEC:
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
        else: self.__dict__[attr]=value #pass through to object attributes
        
    def start(self):
        '''start the job(s) by submitting it to the client
        returns job id/job ids from submit'''
        if self.jid: return False #job already started
        r=list(self.client.submit_job(state='new',**self.info).keys())
        if self.multi:
            self.info={} #clear cache to remove submit data
            self.jid=r
        else: self.jid=r[0]
        self.__getattr__() #sync cache
        if self.notify is not None:
            global _NOTIFY
            if not _NOTIFY: _NOTIFY=Notify()
            _NOTIFY.add(self)
        return self.jid
    
    def kill(self):
        '''stop running job(s)'''
        self.client.kill_jobs(self.jid)
        self.__getattr__() #refresh cache

    def is_alive(self):
        '''returns True if job (or if multi, any jobs) have not finished'''
        if self.multi: return any(active for (jid,active) in self.active.items())
        else: return self.active

    def poll(self):
        '''returns info if a job finished, None if running
        if multi, returns finished jobs or empty dict if none'''
        if self.multi: 
            self.__getattr__() #refresh cache
            return dict((jid,job) for (jid,job) in self.info.items() if not job['active'])
        else:
            if self.is_alive(): return None #refresh and get active flag
            else: return self.info


class Notify(threading.Thread):
    '''keeps running Job objects tracked with the client
    calls the function set in Job.notify when job finishes'''
    def __init__(self):
        self.__jobs=set()
        self.__lock=threading.Lock()
        self.logger=logging.getLogger(name='Notify')
        threading.Thread.__init__(self,daemon=True)
        self.start()

    def run(self):
        self.logger.debug('%s started'%self.name)
        while True:
            with self.__lock:
                for job in self.__jobs:
                    exited=job.poll() #if job(s) exited
                    if exited:
                        #if single job, get id
                        if not job.multi: exited=[job.jid] 
                        #for each exited job, notify and mark as notified
                        for jid in exited:
                            if job.notify and jid not in job.notified:
                                self.logger.debug('%s notify'%jid)
                                job.notify(job) #callback with job object
                                job.notified.add(jid)
                        if not job.is_alive(): #if multi some jobs may still be alive
                            self.logger.debug('removing %s'%job)
                            self.__jobs.remove(job)
                            break #set size changed
            time.sleep(1)

    def add(self,job):
        self.logger.debug('adding %s'%job)
        job.notified=set()
        with self.__lock: self.__jobs.add(job)

    def remove(self,job):
        self.logger.debug('removing %s'%job)
        with self.__lock: 
            if job in self.__jobs: self.__jobs.remove(job)