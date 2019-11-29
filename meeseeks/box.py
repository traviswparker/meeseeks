#!/usr/bin/env python3

import time
import logging
import threading
import uuid
import random
import json
import socket, socketserver, ssl

from .state import State
from .node import Node
from .pool import Pool

class RequestHandler(socketserver.StreamRequestHandler):
    '''control socket request handler'''
    def handle(self):
        self.logger=logging.getLogger(str(self.client_address))
        self.logger.debug('connected')
        while True:
            l=self.rfile.readline() #get line from client
            if not l: break #will be None if client disconnected
            requests=json.loads(l) #apply initial config
            responses=[]
            if requests:
                for request in requests:
                    response=self.server.handler.handle(request)
                    responses.append(response)
            self.wfile.write(json.dumps(responses).encode())
            self.wfile.write('\n'.encode())
            self.wfile.flush() #flush
        self.logger.debug('disconnected')

class RequestListener (socketserver.ThreadingMixIn, socketserver.TCPServer):
    '''control socket'''
    allow_reuse_address=True
    def get_request(self):
        newsocket, fromaddr = self.socket.accept()
        sslcfg=self.cfg.get('ssl')
        if sslcfg:
            connstream = ssl.wrap_socket(newsocket,
                        server_side=True,
                        certfile = sslcfg.get('certfile'),
                        keyfile = sslcfg.get('keyfile'),
                        ca_certs = sslcfg.get('ca_certs') )
            return connstream, fromaddr
        return newsocket, fromaddr

class Box:
    '''meeseeks box main thread'''
    def __init__(self,nodes={},pools={},**cfg):
        #load config defaults
        self.defaults=cfg.get('defaults',{})
        
        #get our nodename
        self.name=cfg.get('name',socket.gethostname())

        #set up logger
        self.logger=logging.getLogger(self.name)

        #init state
        scfg=self.defaults.copy()
        scfg.update(cfg.get('state',{}))
        self.state=State(self.name,**scfg)

        #start listening
        listencfg=self.defaults.copy()
        listencfg.update(cfg.get('listen',{})) #merge in listener specifc options
        self.listener=RequestListener( (  listencfg.get('address','localhost'),
                                            listencfg.get('port',13700)   ), 
                            RequestHandler)
        self.listener.cfg=listencfg #for passing ssl params and other options
        self.listener.handler=self
        self.listener.server_thread=threading.Thread(target=self.listener.serve_forever)
        self.listener.server_thread.daemon=True
        self.listener.server_thread.start()
        self.logger.info('listening on %s:%s'%self.listener.server_address)

        #init pools
        self.pools={} #pools we process jobs for
        for p in pools.keys():
            pcfg=self.defaults.copy()
            pcfg.update(pools[p])
            self.logger.info('creating pool %s'%p)
            self.pools[p]=Pool(self.name,p,self.state,**pcfg)

        #init node threads
        self.nodes={} #nodes we sync
        for n in nodes.keys():
            ncfg=self.defaults.copy()
            ncfg.update(nodes[n])
            self.logger.info('adding node %s'%n)
            self.nodes[n]=Node(self.name,n,self.state,**ncfg)

    def get_loadavg(self):
            with open('/proc/loadavg') as fh:
                try: return float(fh.readline().split()[0])
                except Exception as e: self.logger.warning(e,exc_info=True)

    def biased_random(self,l,reverse=False):
        #this is probably a stupid way to pick a random item while favoring the lowest sorted items 
        #first we pick a range between the first item and a randomly selected item
        #them we pick a random item from that range
        if l: 
            l=sorted(l,reverse=reverse)
            if len(l) > 1: return random.choice( l[ 0:random.randint(1,len(l)) ] )
            else: return l[0] #only one item...

    def select_by_loadavg(self,nodes):
        #loadvg,node sorted low to high
        loadavg_nodes=[ (self.state.node_status[node].get('loadavg'), node) for node in nodes \
            if self.state.node_status[node].get('loadavg') is not None ] 
        #do we have any valid load averages?
        if loadavg_nodes: return self.biased_random(loadavg_nodes)[1]
        #if we have no valid load averages, pick a random node from the pool
        else: return random.choice( nodes )
    
    def select_by_available(self,pool,nodes):
        #get nodes in this pool sorted from most to least open slots
        nodes_slots=[ (self.state.pool_status[pool][node], node) for node in nodes \
            if self.state.pool_status[pool][node] is not None ]
        #do we have any nodes with pool slots?
        if nodes_slots: 
            s,node=self.biased_random(nodes_slots,reverse=True)
            if s > 0: self.state.update_pool_status(pool,node,s-1) #update the local free slot count
            return node
        #if not, pick a random node from the pool
        else: return random.choice( nodes )

    def run(self):
        self.logger.info('running')
 
        while True: #existence is pain!
            #update our node status
            self.state.update_node_status( self.name,
                online=True,
                ts=time.time(),
                loadavg=self.get_loadavg() )

            #job routing logic
            try:
                #get jobs assigned to us
                for jid,job in self.state.get(node=self.name).items():
                    pool=job['pool']
                    #if we can service this job, the pool thread will claim the job so do nothing
                    if pool not in self.pools: 
                        try:
                            #we need to select a node that has the job's pool
                            nodes=list(self.state.pool_status.get(pool,{}).keys())
                            if not nodes: continue #we can't do anything with this job

                            #filter by the job's nodelist if set
                            if job['nodelist']: 
                                in_list_nodes=[node for node in nodes if node in job['nodelist']]
                                #if we got a result, use it
                                #we may not if the nodelist only controlled the upstream routing
                                #so if we got nothing based on the node list use all pool nodes
                                if in_list_nodes: nodes=in_list_nodes

                            #select a node the job
                            if self.defaults.get('use_loadavg'): node=self.select_by_loadavg(nodes)
                            else: node=self.select_by_available(pool,nodes)

                            #route the job
                            self.logger.debug('routing %s for %s to %s'%(jid,pool,node))
                            self.state.update_job(jid,node=node)
                            
                        except Exception as e: self.logger.warning(e,exc_info=True)
                
                time.sleep(1)

            except KeyboardInterrupt: break
            except Exception as e: self.logger.error(e,exc_info=True)

        self.logger.info('shutting down')
        try:
            self.listener.shutdown()
            self.listener.server_thread.join()
        except Exception as e: self.logger.error(e,exc_info=True)

        #stop all pools
        for pool in self.pools.values():
            pool.shutdown.set()
            self.logger.info('stopping %s'%pool.name)
            pool.join()

        #stop all nodes
        for node in self.nodes.values():
            node.shutdown.set()
            self.logger.info('stopping %s'%node.name)
            node.join()

        #stop state manager
        self.state.shutdown.set()
        self.logger.info('stopping %s'%self.state.name)
        self.state.join()    

    #handle incoming request
    def handle(self,request):
        response={}
        #we're being pushed state from upstream node and should return ours
        if 'sync' in request:
            #sync incoming state, return updated job ids
            response['sync']=self.state.sync(request['sync'])
        #return our state
        if 'get' in request:
            response['get']=self.state.get(**request['get'])
        #submit job
        if 'submit' in request: 
            response['submit']=self.state.add_job(**request['submit'])
        #query job
        if 'query' in request: response['query']=self.state.get_job(request['query'])
        #modify job
        if 'modify' in request:
            for jid,data in request['modify'].items():
                response.setdefault('modify',{})[jid]=self.state.update_job(jid,**data)
        #kill job
        if 'kill' in request:
                response['kill']=self.state.update_job(request['kill'],state='killed')
        #get cluster status
        if 'status' in request: 
            response['status']={     
                #return the status of us and downstream nodes
                'nodes':self.state.node_status,
                #return the  status of pools we know about
                'pools':self.state.pool_status
            }
        return response