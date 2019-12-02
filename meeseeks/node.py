#!/usr/bin/env python3

import time
import threading
import logging
import json
import socket, ssl

class Node(threading.Thread):
    '''node poller/state sync thread
    initially we try to push all state to the node (sync_ts of 0)'''
    def __init__(self,node,remote_node,state,refresh=10,address=None,port=13700,timeout=10,**cfg):
        self.node=node #node we are running on 
        self.remote_node=remote_node #node we connect to
        self.state=state
        threading.Thread.__init__(self,daemon=True,name='Node.'+self.remote_node,target=self.node_run)
        self.logger=logging.getLogger(self.name)
        self.address=address
        if not self.address: self.address=self.node
        self.port=port
        self.timeout=timeout
        self.refresh=refresh
        self.__socket=None
        self.cfg=cfg
        self.shutdown=threading.Event()
        self.start()

    def __sr(self,requests):
        #conected and send/recieve request/response
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

    def node_run(self):
        while not self.shutdown.is_set():
            if not self.__socket: ts=0 #reset ts to push all state on reconnect
            
            #we sync updates for all nodes that are routed through the remote node
            sync=dict( (jid,job) for (jid,job) in self.state.get(ts=ts).items() \
                        if job['node'] != self.node and job['node'] in \
                        self.status.node_state.get(self.remote_node,{}).get('seen',[])
                    )
            #create the request
            request={
                'status':{},
                #dump all jobs for this node updated more recently than the last sync
                'sync':sync,
                'get':{'ts':ts}
            }
            responses=self.__sr([request])
            updated=None
            if responses:
                response=responses[0]
                updated=self.state.sync(
                    response.get('get',{}),
                    response.get('status',{}),
                    remote_node=self.remote_node)
            if responses: self.logger.debug('%s sent %s, updated %s'%(ts,len(sync),len(updated)))
            ts=time.time()-self.refresh #set the next window to go back to one refresh period ago
            time.sleep(self.refresh) 
        if self.__socket:self.__socket.close()