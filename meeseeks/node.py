#!/usr/bin/env python3

import time
import threading
import logging
import json
import socket

from .util import create_ssl_context

class Node(threading.Thread):
    '''node poller/state sync thread
    initially we try to push all state to the node (sync_ts of 0)'''
    def __init__(self,__node,__remote_node,__state,start_node=True,**cfg):
        self.node=__node #node we are running on 
        self.remote_node=__remote_node #node we connect to
        self.state=__state
        name='Node'
        if self.node:name=self.node+'.'+name
        if self.remote_node: name+='.'+self.remote_node
        threading.Thread.__init__(self,daemon=True,name=name,target=self.__node_run)
        self.logger=logging.getLogger(self.name)
        self.__lock=threading.Lock() #to ensure direct request and sync don't clobber
        self.__socket=None
        self.shutdown=threading.Event()
        self.sync=threading.Event()
        self.refresh=0
        self.config(**cfg)
        if self.refresh: self.start() #if refresh=0, do not start thread

    def config(self,address=None,port=13700,timeout=10,refresh=1,poll=10,**cfg):
        if address: self.address=address
        else: self.address=self.node
        if port: self.port=int(port)
        if timeout: self.timeout=int(timeout)
        if refresh: self.refresh=int(refresh) #how often we sync the remote node
        if self.refresh and poll: self.poll_count=int(poll)/self.refresh
        self.cfg=cfg

    def request(self,requests):
        with self.__lock:
            #connect and send/recieve request/response
            if not self.__socket:
                self.logger.debug('connecting to %s:%s'%(self.address,self.port))
                try: 
                    self.__socket=socket.create_connection((self.address,self.port),timeout=self.timeout)
                    if 'ssl' in self.cfg:
                        self.__socket = create_ssl_context(self.cfg.get('ssl')).wrap_socket(self.__socket)
                except Exception as e:
                    self.logger.debug(e)
                    if self.__socket is not False:
                        self.logger.warning(e)
                        self.__socket=False #suppress repeated warnings
                if self.__socket: self.logger.info('connected to %s:%s'%(self.address,self.port))
            if self.__socket:
                try:
                    self.__socket.sendall(json.dumps(requests).encode())
                    self.__socket.sendall('\n'.encode())
                    l=''
                    while True:
                        d=self.__socket.recv(65535).decode()
                        if not d: raise Exception('disconnected from %s:%s'%(self.address,self.port))
                        l+=d
                        if '\n' in l: return json.loads(l)
                except Exception as e: 
                    self.logger.warning(e)
                    if self.__socket: 
                        self.__socket.close()
                        self.__socket=None

    def __node_run(self):
        while not self.shutdown.is_set():
            if not self.__socket: #reset sync on disconnect
                local_seq=remote_seq=0
                poll=-1
            
            #we sync updates for all nodes that are routed through the remote node
            #if self.node is None, we are are a client and always send updates
            node_status=self.state.get_nodes()
            sync=dict( (jid,job) for (jid,job) in self.state.get(seq=local_seq).items() \
                        if self.node is None or \
                            job['node'] in node_status.get(self.remote_node,{}).get('routing',[])
                    )
            #get highest local sequence number
            if sync: local_seq=max(job['seq'] for job in sync.values())
            #create the request
            req={
                #dump all jobs for this node updated more recently than the last sync
                'sync':sync,
                'get':{'seq':remote_seq}
            }

            #get status if poll interval
            poll=(poll+1)%self.poll_count
            if not poll: req.update(nodes={}) 
            
            #make request
            responses=self.request([req])
            updated=None
            #sync incoming state
            if responses:
                response=responses[0]
                jobs=response.get('get',{})
                #get highest remote seq number
                if jobs: remote_seq=max(job['seq'] for job in jobs.values())
                #get node status
                status=response.get('nodes',{})
                updated=self.state.sync(jobs,status,remote_node=self.remote_node)
                self.logger.debug('%s sent %s, updated %s, local_seq %s, remote_seq %s'%
                    (time.time(),len(sync),len(updated),local_seq,remote_seq)    )
                #toggle the sync Event to signal anything waiting for sync
                self.sync.set()
                self.sync.clear()
            time.sleep(self.refresh) 
        if self.__socket:self.__socket.close()