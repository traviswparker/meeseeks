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
j=c.add_job(pool=...,args=[.....])  #submit a job, return the ID
c.get_job(j)        #check the job
c.kill_job(j)       #stop the job
c.close()           #disconnect client
'''

class Client(State):
    '''client class to connect to a node and manage the state of it and all downstream nodes.
        State object's methods are available to get status and manage jobs
        submit and kill_job methods are also available to make direct requests'''
    def __init__(self,address=None,port=13700,timeout=10,refresh=1,poll=10,expire=60,**cfg):

        #state object to cache cluster state from the node
        State.__init__(self,None,
                        refresh=refresh,
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
    def submit(self,**kwargs): return self.__node.request([{'submit':kwargs}])[0]['submit']
    def query(self,jid): return self.__node.request([{'query':jid}])[0]['query']
    def kill(self,jid): return self.__node.request([{'kill':jid}])[0]['kill']
    def ls(self,**kwargs): return self.__node.request([{'ls':kwargs}])[0]['ls']
    def status(self): return self.__node.request([{'status':None}])[0]['status']
    
    def close(self): 
        #stop the node and state threads
        self.__node.shutdown.set()
        self.__node.join()
        self.shutdown.set()
        self.join()


