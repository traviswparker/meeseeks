#!/usr/bin/env python3

import time
import threading
import logging

from .client import Client

class Node(threading.Thread):
    '''node poller/state sync thread
    initially we try to push all state to the node (sync_ts of 0)'''
    def __init__(self,node,remote_node,state,refresh=10,address=None,port=13700,timeout=10,**cfg):
        self.node=node #node we are running on 
        self.remote_node=remote_node #node we connect to
        self.state=state
        threading.Thread.__init__(self,daemon=True,name='Node.'+self.remote_node,target=self.node_run)
        self.logger=logging.getLogger(self.name)
        if not address: address=self.node
        self.refresh=refresh
        self.shutdown=threading.Event()
        self.__client=Client(
            remote_node=remote_node,
            address=address,
            port=port,
            timeout=timeout,
            **cfg)
        self.start()

    def node_run(self):
        ts=0
        while not self.shutdown.is_set():
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

            responses=self.__client.request([request])
            updated=None
            if responses:
                response=responses[0]
                updated=self.state.sync(
                    response.get('get',{}),
                    response.get('status',{}),
                    remote_node=self.remote_node)
                self.logger.debug('%s sent %s, updated %s'%(ts,len(sync),len(updated)))
                ts=time.time()-self.refresh #set the next window to go back to one refresh period ago
            else: ts=0 #if client did not respond, reset ts to sync all on reconnect
            time.sleep(self.refresh) 
        self.__client.close()
