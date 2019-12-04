#!/usr/bin/env python3

import time
import logging
import threading
import uuid
import random
import json
import socket
import socketserver
import ssl

from .state import State
from .node import Node, create_ssl_context
from .pool import Pool

def cmdline_parser(args):
    #parse args
    # cfg={key[.subkey]=value[,value..] args preceeding first non = argument}
    cfg={}
    for i,arg in enumerate(args):
        if '=' in arg:
            k,v=arg.split('=',1)
            s=None #sub dict, for k.s=v arguments
            if '.' in k:
                k,sk=k.split('.',1)
                s=cfg.setdefault(k,{})
            if v.isnumeric(): v=int(v)
            elif ',' in v: v=list(v.split(','))
            if s is not None: s[sk]=v
            else: cfg[k]=v
        else: 
            args=args[i:]
            break
    return cfg,args

class RequestHandler(socketserver.StreamRequestHandler):
    '''control socket request handler'''
    def handle(self):
        self.logger=logging.getLogger(str(self.client_address))
        self.logger.debug('connected')
        while True:
            l=self.rfile.readline() #get line from client
            if not l.strip(): break #will be None if client disconnected
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
        self.pools={}
        self.nodes={}

        self.shutdown=threading.Event()
        self.restart=threading.Event()

        #load config defaults
        self.defaults=cfg.get('defaults',{})
        
        #get our nodename
        self.name=cfg.get('name',socket.gethostname())

        #set up logger
        self.logger=logging.getLogger(self.name)

        #init state
        scfg=self.defaults.copy()
        scfg.update(self.cfg.get('state',{}))
        self.state=State(self.name,**scfg)

        #start listening
        lcfg=self.defaults.copy()
        lcfg.update(self.cfg.get('listen',{})) #merge in listener specifc options
        self.listener=RequestListener( (  lcfg.get('address','localhost'),
                                            lcfg.get('port',13700)   ), 
                                        RequestHandler)
        if 'ssl' in lcfg: self.listener.ssl_context=create_ssl_context(lcfg['ssl'])
        self.listener.handler=self
        self.listener.server_thread=threading.Thread(target=self.listener.serve_forever)
        self.listener.server_thread.daemon=True
        self.listener.server_thread.start()
        self.logger.info('listening on %s:%s'%self.listener.server_address)

    def apply_config(self):
        #stop/init pools
        pools=self.cfg.get('pools',{})
        for p in self.pools.copy(): 
            if p not in pools: self.stop_pool(p)
        for p in pools.keys():
            if p not in self.pools:
                pcfg=self.defaults.copy()
                pcfg.update(pools[p])
                self.logger.info('creating pool %s'%p)
                self.pools[p]=Pool(self.name,p,self.state,**pcfg)

        #stop/init nodes
        nodes=self.cfg.get('nodes',{})
        for n in self.nodes.copy(): 
            if n not in nodes: self.stop_node(n)
        for n in nodes.keys():
            if n not in self.nodes:
                ncfg=self.defaults.copy()
                ncfg.update(nodes[n])
                self.logger.info('adding node %s'%n)
                self.nodes[n]=Node(self.name,n,self.state,**ncfg)

    #stop all pool
    def stop_pool(self,p):
        pool=self.pools[p]
        pool.shutdown.set()
        self.logger.info('stopping %s'%pool.name)
        pool.join()
        del self.pools[p]

    #stop all node
    def stop_node(self,n):
        node=self.nodes[n]
        node.shutdown.set()
        self.logger.info('stopping %s'%node.name)
        node.join()
        del self.nodes[n]

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
        node_status=self.state.get_node_status()
        loadavg_nodes=[ (node_status[node].get('loadavg'), node) for node in nodes \
            if node_status[node].get('loadavg') is not None ] 
        #do we have any valid load averages?
        if loadavg_nodes: return self.biased_random(loadavg_nodes)[1]
        #if we have no valid load averages, pick a random node from the pool
        else: return random.choice( nodes )
    
    def select_by_available(self,pool,nodes):
        #get nodes in this pool sorted from most to least open slots
        pool_status=self.state.get_pool_status()
        nodes_slots=[ (pool_status[pool][node], node) for node in nodes \
            if pool_status[pool][node] is not None ]
        #do we have any nodes with pool slots?
        if nodes_slots: 
            s,node=self.biased_random(nodes_slots,reverse=True)
            if s > 0: self.state.update_pool_status(pool,node,s-1) #update the local free slot count
            return node
        #if not, pick a random node from the pool
        else: return random.choice( nodes )

    def run(self):
        self.logger.info('starting')
        while not self.shutdown.is_set():  #existence is pain!
            self.apply_config()
            self.restart.clear()
            while not self.shutdown.is_set() and not self.restart.is_set():
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
                                nodes=list(self.state.get_pool_status().get(pool,{}).keys())
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

                except Exception as e: 
                    self.logger.error(e,exc_info=True)
                    self.shutdown.set()

        self.logger.info('shutting down')
        #will stop all pools/nodes
        self.cfg.update(pools={},nodes={})
        self.apply_config() 
        try:
            self.listener.shutdown()
            self.listener.server_thread.join()
        except Exception as e: self.logger.error(e,exc_info=True)

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
        if 'query' in request:
            response['query']=self.state.get_job(request['query'])
        #modify job
        if 'modify' in request:
            for jid,data in request['modify'].items():
                response.setdefault('modify',{})[jid]=self.state.update_job(jid,**data)
        #kill job
        if 'kill' in request:
            response['kill']=self.state.update_job(request['kill'],state='killed')

        # List all jobs
        if 'ls' in request:
            response['ls']=self.state.list_jobs(**request['ls'])

        #get cluster status
        if 'status' in request: 
            response['status']={     
                #return the status of us and downstream nodes
                'nodes':self.state.get_node_status(),
                #return the  status of pools we know about
                'pools':self.state.get_pool_status()
            }

        # Does not format nicely via netcat, because of newlines/tabs
        if 'options' in request:
            if 'pretty' in request['options'] and request['options']['pretty']:
                response = json.dumps(response, sort_keys=True, indent=4)

        #get/set config
        if 'config' in request:
            cfg=request['config']
            if cfg: #if changes were pushed
                self.cfg.update(request['config'])
                self.restart.set() #main loop breaks and apply_config is called
            response['config']=self.cfg 

        return response