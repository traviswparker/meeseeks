#!/usr/bin/env python3

import time
import threading
import logging
import json
import socket, ssl

class Node(threading.Thread):
    '''node poller/state sync thread
    initially we try to push all state to the node (sync_ts of 0)'''
    def __init__(self,node,remote_node,state,address=None,port=13700,timeout=10,refresh=1,window=5,poll=10,**cfg):
        self.node=node #node we are running on 
        self.remote_node=remote_node #node we connect to
        self.state=state
        name='Node'
        if self.remote_node: name+='.'+self.remote_node
        threading.Thread.__init__(self,daemon=True,name=name,target=self.__node_run)
        self.logger=logging.getLogger(self.name)
        self.address=address
        if not self.address: self.address=self.node
        self.port=port
        self.timeout=timeout
        self.refresh=refresh #how often we sync the remote node
        self.window=window #how far back (in refresh periouds) we request sync data
        self.poll=poll
        self.__lock=threading.Lock() #to ensure direct request and sync don't clobber
        self.__socket=None
        self.cfg=cfg
        self.shutdown=threading.Event()
        self.start()

    def request(self,requests):
        with self.__lock:
            #connect and send/recieve request/response
            if not self.__socket:
                self.logger.debug('connecting to %s:%s'%(self.address,self.port))
                try: 
                    self.__socket=socket.create_connection((self.address,self.port),timeout=self.timeout)
                    sslcfg=self.cfg.get('ssl')
                    if sslcfg:
                        self.__socket = ssl.wrap_socket(self.__socket,
                            certfile = sslcfg.get('certfile'),
                            keyfile = sslcfg.get('keyfile'),
                            ca_certs = sslcfg.get('ca_certs') )
                except Exception as e:
                    if self.__socket is not False:
                        self.logger.warning("%s:%s %s"%(self.address,self.port,e))
                        self.__socket=False #suppress repeated warnings
                if self.__socket: self.logger.info('connected to %s:%s'%(self.address,self.port))
            if self.__socket:
                try:
                    self.__socket.sendall(json.dumps(requests).encode())
                    self.__socket.sendall('\n'.encode())
                    l=''
                    while True:
                        l+=self.__socket.recv(65535).decode()
                        if '\n' in l: return json.loads(l)
                except Exception as e: 
                    self.logger.warning(e,exc_info=True)
                    if self.__socket: 
                        self.__socket.close()
                        self.__socket=None

    def __node_run(self):
        while not self.shutdown.is_set():
            if not self.__socket: ts=poll=0 #reset ts to sync all state on reconnect
            
            #we sync updates for all nodes that are routed through the remote node
            #if self.node is None, we are are a client and always send updates
            node_status=self.state.get_node_status()
            sync=dict( (jid,job) for (jid,job) in self.state.get(ts=ts).items() \
                        if self.node is None or \
                         ( job['node'] != self.node and job['node'] in \
                           node_status.get(self.remote_node,{}).get('seen',[] ) )
                    )
            #create the request
            req={
                #dump all jobs for this node updated more recently than the last sync
                'sync':sync,
                'get':{'ts':ts}
            }
            if not (poll%self.poll): req.update(status=None) #get status if poll interval
            poll+=1
            responses=self.request([req])
            updated=None
            if responses:
                response=responses[0]
                updated=self.state.sync(
                    response.get('get',{}),
                    response.get('status',{}),
                    remote_node=self.remote_node)
            if responses: self.logger.debug('%s sent %s, updated %s'%(ts,len(sync),len(updated)))
            ts=time.time()-(self.refresh*self.window) #set the next window to go back two refresh periods ago
            time.sleep(self.refresh) 
        if self.__socket:self.__socket.close()