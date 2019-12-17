#1/usr/bin/env python3

import time
import threading
import logging

from .state import State
from .node import Node

'''
Example:
from meeseeks import Client
c=Client('localhost')
c.get_node_status() #get cluster status
c.get_pool_status() #get pools
c.get()             #get all jobs
j=c.submit_job(pool=...,args=[.....])  #submit a job, return the ID
c.get_job(j)        #check the job
c.kill_jobs(j)       #stop the job(s)
c.close()           #disconnect client
'''

class Client(State):
    '''client class to connect to a node and manage the state of it and all downstream nodes.
        State object's methods are available to get status and manage jobs
        methods are also available to make direct requests'''
    def __init__(self,address=None,port=13700,timeout=10,refresh=1,poll=10,expire=60,**cfg):

        #state object to cache cluster state from the node
        State.__init__(self,None,
                        expire=expire,
                        expire_active_jobs=False)
     
        #internal node object to communicate with the node
        self.__node=Node(None,None,self,
                        address=address,
                        port=port,
                        timeout=timeout,
                        refresh=refresh,
                        poll=poll,
                        **cfg)

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
    def status(self,**kwargs): return self.__node.request([{'status':kwargs}])[0]['status']
    
    #for sending raw requests
    def request(self,req): return self.__node.request([req])[0]
    
    def close(self): 
        #stop the node and state threads
        self.__node.shutdown.set()
        self.__node.join()
        self.shutdown.set()
        self.join()


