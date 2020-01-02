#!/usr/bin/env python3

import time
import logging
import threading
import uuid
import random
import json
import socket
import socketserver

from .state import State
from .node import Node
from .pool import Pool
from .util import create_ssl_context, import_plugin

class RequestHandler(socketserver.StreamRequestHandler):
    '''control socket request handler'''
    def handle(self):
        self.logger=logging.getLogger(str(self.client_address))
        self.logger.debug('connected')
        while not self.server.handler.shutdown.is_set(): #client will be disconnected at shutdown
            l=self.rfile.readline() #get line from client
            if not l.strip(): break #will be None if client disconnected
            responses=[]
            try:
                requests=json.loads(l) #apply initial config
                if requests:
                    for request in requests:
                        response=self.server.handler.handle(request)
                        responses.append(response)
            except Exception as e: responses.append({'error':str(e)})
            self.wfile.write(json.dumps(responses).encode())
            self.wfile.write('\n'.encode())
            self.wfile.flush() #flush
        self.logger.debug('disconnected')

class RequestListener (socketserver.ThreadingMixIn, socketserver.TCPServer):
    '''control socket'''
    allow_reuse_address=True
    ssl_context=None
    def get_request(self):
        newsocket, fromaddr = self.socket.accept()
        if self.ssl_context:
            connstream = self.ssl_context.wrap_socket(newsocket,server_side=True )
            return connstream, fromaddr
        return newsocket, fromaddr

class Box:
    '''meeseeks box main thread'''
    def __init__(self,**cfg):
        self.cfg=cfg
        self.state=self.listener=None
        self.pools={}
        self.nodes={}
        self.shutdown=threading.Event()
        self.restart=threading.Event()        
        #get our nodename
        self.name=cfg.get('name',socket.gethostname())
        #set up logger
        self.logger=logging.getLogger(self.name)

    def apply_config(self,**cfg):
        self.logger.info('reloading config')
        self.cfg.update(cfg)
        #load config defaults
        self.defaults=self.cfg.get('defaults',{})

        #init state
        scfg=self.defaults.copy()
        scfg.update(self.cfg.get('state',{}))
        if not self.state: self.state=State(self.name,**scfg)
        else: self.state.config(**scfg)

        #stop/init pools
        pools=self.cfg.get('pools',{})
        for p in self.pools.copy(): 
            if p not in pools: self.stop_pool(p)
        for p in pools.keys():
            pcfg=self.defaults.copy()
            pcfg.update(pools[p])
            #load plugin if specified
            if 'plugin' in pcfg:
                pool_class=import_plugin(pcfg['plugin'])
                del pcfg['plugin']
            else: pool_class=Pool
            if p not in self.pools:
                self.logger.info('creating %s %s'%(pool_class.POOL_TYPE,p))
                self.pools[p]=pool_class(self.name,p,self.state,**pcfg)
            else: self.pools[p].config(**pcfg)

        #stop/init nodes
        nodes=self.cfg.get('nodes',{})
        for n in self.nodes.copy(): 
            if n!=self.name and n not in nodes: self.stop_node(n)
        for n in nodes.keys():
            if n==self.name: continue #we don't need to talk to ourself 
            ncfg=self.defaults.copy()
            ncfg.update(nodes[n])
            if n not in self.nodes:
                self.logger.info('adding node %s'%n)
                self.nodes[n]=Node(self.name,n,self.state,**ncfg)
            else: self.nodes[n].config(**ncfg)

    #stop and remove pool
    def stop_pool(self,p):
        pool=self.pools[p]
        pool.shutdown.set()
        self.logger.info('stopping %s'%pool.name)
        pool.join()
        del self.pools[p]

    #stop and remove node connection
    def stop_node(self,n):
        node=self.nodes[n]
        node.shutdown.set()
        self.logger.info('stopping %s'%node.name)
        node.join()
        del self.nodes[n]
        self.state.update_node(n,online=False,remove=True)

    def get_loadavg(self):
        try:
            with open('/proc/loadavg') as fh:
                return float(fh.readline().split()[0])
        except: return 0.0 #only works on linux... ignore it.

    def biased_random(self,l,reverse=False):
        #this is probably a stupid way to pick a random item while favoring the first of the sorted items 
        #first we pick a range between the first item and a randomly selected item
        #them we pick a random item from that range
        if l: 
            l=sorted(l,reverse=reverse)
            if len(l) > 1: return random.choice( l[ 0:random.randint(1,len(l)) ] )
            else: return l[0] #only one item...

    def select_by_loadavg(self,node_status,nodes):
        #loadvg,node sorted low to high
        loadavg_nodes=[ (node_status[node].get('loadavg'), node) for node in nodes \
            if node_status[node].get('loadavg') is not None ] 
        #do we have any valid load averages?
        if loadavg_nodes: return self.biased_random(loadavg_nodes)[1]
        #if we have no valid load averages, pick a random node from the pool
        else: return random.choice( nodes )
    
    def select_by_available(self,pool_status,nodes):
        #get nodes in this pool sorted from most to least open slots
        #exclude nodes in pool with no free slots unless we have no free slots
        nodes_slots=[ (pool_status[node], node) for node in nodes if \
            (pool_status[node]>0 and pool_status[node] is not True) ]
        #do we have any nodes with pool slots?
        if nodes_slots: 
            s,node=self.biased_random(nodes_slots,reverse=True)
            return node
        #if not, pick a random node from the pool
        else: return random.choice( nodes )

    def run(self):
        while not self.shutdown.is_set():  #existence is pain!

            #apply state/pool/node config
            self.apply_config()

            #start listener
            if not self.listener:
                lcfg=self.defaults.copy()
                lcfg.update(self.cfg.get('listen',{})) #merge in listener specifc options
                port,address,prefix=lcfg.get('port',13700),lcfg.get('address'),lcfg.get('prefix')
                #if we are given a prefix instead of an address
                #find the address that matches the prefix and listen on it
                if not address:
                    address='localhost'
                    if prefix:
                        addrs=(t[4][0] for t in socket.getaddrinfo(socket.gethostname(),port))
                        for addr in addrs:
                            if addr.startswith(prefix): address=addr
                self.listener=RequestListener((address,port),RequestHandler)
                if 'ssl' in lcfg: self.listener.ssl_context=create_ssl_context(lcfg['ssl'])
                self.listener.handler=self
                self.listener.server_thread=threading.Thread(target=self.listener.serve_forever)
                self.listener.server_thread.daemon=True
                self.listener.server_thread.start()
                self.logger.info('listening on %s:%s'%self.listener.server_address)

            self.restart.clear()
            while not self.shutdown.is_set() and not self.restart.is_set():
                #update our node status
                #we can route to nodes we see via our connected nodes
                self.state.update_node( self.name,
                    online=True,
                    ts=time.time(),
                    loadavg=self.get_loadavg(),
                    routing=list(self.state.get_nodes().keys()) ) 

                #job routing logic
                try:
                    #handle config jobs assigned to us
                    for jid,job in self.state.get(node=self.name,pool='__config').items():
                        if job['state'] == 'new':
                            self.logger.info('got config %s: %s'%(jid,job))
                            if job['args']: #if changes were pushed
                                self.cfg.update(job['args'])
                                self.restart.set() #main loop breaks and apply_config is called
                            #return current config in args
                            self.state.update_job(jid,args=self.cfg,state='done')

                    #get jobs assigned to us but to a pool we don't have
                    #these need to be assigned to a node that can service them
                    jobs=dict( (jid,job) for (jid,job) in self.state.get(node=self.name).items() \
                        if job['pool'] not in self.pools.keys() )
                    #add jobs without node assigned, these were just submitted and need routing
                    jobs.update(self.state.get(node=False))
                    node_status=self.state.get_nodes()
                    #process jobs by least recently updated to most recently updated
                    for jid,job in sorted(jobs.items(),key=lambda j:j[1]['ts']):
                        try:
                            #if no submit node, we're the first to handle it so set if we're a node
                            if not job.get('submit_node'): self.state.update_job(jid,submit_node=self.name)
                            pool=job['pool']
                            pool_status=self.state.get_pools().get(pool,{})
                            #we need to select a node:
                            # with open slots (slots > 0)
                            # or full (slots is < 1) but jobs can wait
                            # or without defined slots (slots is True)
                            nodes=[node for node,free_slots in pool_status.items() if free_slots>0 or self.cfg.get('wait_in_pool')]

                            #if no nodes or job in hold, we can't route this job yet
                            if not nodes or (job.get('hold') and not self.cfg.get('wait_in_pool')):
                                if not job['node']: self.state.update_job(jid,node=self.name) #assign to us for now
                                continue 

                            #select a node for the job
                            if self.cfg.get('use_loadavg'): node=self.select_by_loadavg(node_status,nodes)
                            else: node=self.select_by_available(pool_status,nodes)

                            #route the job
                            slots=pool_status[node]
                            self.logger.info('routing %s for %s to %s (%s)'%(jid,pool,node,slots))
                            self.state.update_job(jid,node=node)
                            
                        except Exception as e: self.logger.warning(e,exc_info=True)
                    
                    time.sleep(1)

                except Exception as e: 
                    self.logger.error(e,exc_info=True)
                    self.shutdown.set()

        self.logger.info('shutting down')
        #will stop all pools/nodes
        self.cfg.update(pools={},nodes={})
        self.apply_config() 

        #stop state manager
        self.state.shutdown.set()
        self.logger.info('stopping %s'%self.state.name)
        self.state.join()   

        #stop listening
        self.listener.shutdown()
        self.listener.server_thread.join()

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
        #submit or modify a job
        if 'submit' in request: 
            response['submit']=self.state.submit_job(**request['submit'])
        #get job by id
        if 'job' in request:
            response['job']=self.state.get_job(request['job'])
        #modify job - this bypasses all checks, use submit with id=existing if possible
        if 'modify' in request:
            for jid,data in request['modify'].items():
                response.setdefault('modify',{})[jid]=self.state.update_job(jid,**data)
        #kill job
        if 'kill' in request:
            response['kill']=self.state.kill_jobs(request['kill'])
        # List all jobs
        if 'ls' in request:
            response['ls']=self.state.list_jobs(**request['ls'])
        #return the status of us and downstream nodes
        if 'nodes' in request: response['nodes']=self.state.get_nodes()  
        if 'pools' in request: response['pools']=self.state.get_pools()  
        #get/set config
        if 'config' in request:
            cfg=request['config']
            if cfg: #if changes were pushed
                self.logger.info('got config request: %s'%cfg)
                self.cfg.update(request['config'])
                self.restart.set() #main loop breaks and apply_config is called
            response['config']=self.cfg 
        # Does not format nicely via netcat, because of newlines/tabs
        if 'options' in request:
            if 'pretty' in request['options'] and request['options']['pretty']:
                response = json.dumps(response, sort_keys=True, indent=4)
        return response