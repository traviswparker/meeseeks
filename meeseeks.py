import sys, os, time
import logging
import threading, subprocess
import uuid
import json
import socket, socketserver, ssl

#
#Utility functions
#

def import_package(pkg):
    '''imports a package and places the exported modules in the global namespace
    returns packagename:[imports]'''
    m=importlib.import_module(pkg)
    try: attrlist = m.__all__
    except AttributeError: attrlist = dir (m)
    except Exception as e: return e
    for attr in attrlist: globals()[attr] = getattr (m, attr)
    return {pkg:attrlist}

def parse_list(v): 
    '''parses a comma sep list to stripped strings'''
    return [p.strip() for p in v.split(',')]

def parse_int(v): 
    '''parses a comma sep list of integers'''
    return [int(p.strip()) for p in v.split(',')]

def parse_env(v): 
    '''replace environment vars in values'''
    v=v.split(os.path.sep) #split paths to find embedded env vars
    #build line   as path      with substuted vars                  or literal parts
    v=os.path.sep.join(os.environ.get(e[1:],'') if e.startswith('$') else e for e in v )
    try: #parse numerics as integers 
        v=parse_int(v)
        if len(v)==1: v=v[0]
    except: pass
    return v

def parse_line(l):
    '''remove comments and split line into words
    detects largest quoted section and preserves contained whitespace'''
    start=end=None
    l=l.strip().split('#')[0].split() #remove comments and split line
    #detect quoted sections and join with whitespace into one arg
    start=[i for i,w in enumerate(l) if w.startswith('"')]
    if start:
        start=start[0] #get leftmost start quote position
        end=[i for i,w in enumerate(l[start:]) if w.endswith('"')]
        if end: 
            end=start+end[-1]+1 #get rightmost end quote pos
            #return line before quote start, quoted with quotes removed, line after
            return l[:start] + [' '.join(l[start:end]).replace('"','')] + l[end:]
    return l #no quotes in line

def parse_kvs(kvs,f=None,parse_value=None):
    '''parse for key=value
    f: limit to set of keys in f
    parse_value: 
        If None (default), will attempt to parse ints, lists of ints, and environment vars
        If True, will also split strings at commas and always return list
        If False, will not split or parse value'''
    d={}
    if kvs:
        for kv in kvs:
            try: 
                k,v=kv.split('=',1)
                k,v=k.strip(),v.strip()
                if parse_value is not False:
                    try: v=parse_int(v) #will create list of ints
                    except: v=parse_env(v) #will populate $env vars in values
                    if parse_value is True: #values will always be alist of strings or ints
                        try: v=parse_list(v) #turn all strings into lists
                        except: pass #not a string, already parsed
                    elif type(v) is list and len(v)==1: v=v[0] #don't leave single integer lists as lists
            except: k,v=kv.strip(),None #did not parse as k=v, treat as bare key
            if f and (k not in f): continue #filter by key if filter given
            d[k]=v
    return d

def parse_result(r):
    '''turn single non-null results into list'''
    if (r is not None) and (type(r) is not list): r=[r]
    return r


class RequestHandler(socketserver.StreamRequestHandler):
    '''control socket request handler'''
    def handle(self):
        self.__logger=logging.getLogger(str(self.client_address))
        self.__logger.info('connected')
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
        self.__logger.info('disconnected')

class RequestListener (socketserver.ThreadingMixIn, socketserver.TCPServer):
    '''control socket'''
    allow_reuse_address=True
    def get_request(self):
        newsocket, fromaddr = self.socket.accept()
        if 'ssl' in self.kwargs:
            connstream = ssl.wrap_socket(newsocket,
                        server_side=True,
                        certfile = self.kwargs.get('certfile'),
                        keyfile = self.kwargs.get('keyfile'),
                        ca_certs = self.kwargs.get('ca_certs') )
            return connstream, fromaddr
        return newsocket, fromaddr


    
class State(threading.Thread):
    '''cluster state handler'''
    def __init__(self,*args,**kwargs):
        self.__name=kwargs.get('name',socket.gethostname())
        threading.Thread.__init__(self,daemon=True,name=self.__name+'.state')
        self.__logger=logging.getLogger(self.name)

        self.__lock=threading.Lock()
        self.shutdown=threading.Event()

        self.__expire=kwargs.get('expire',60)
    
        '''state is:
            __jobs: { jid: {pool: node: ts: state: [jobargs...] }} }
            jid: uuid of the job
            ts: the last updated timestamp of the job
            pool: the pool (queue) the job runs in 
            node: the node the job is assigned to
            state: the job state
        '''

        self.__jobs={} #us and nodes we connect directly to.
    
        self.start()

    def dump(self,node=None,pool=None,ts=None):
        #dump all state for a node/pool/or updated after a certain ts
        with self.__lock:
            try: 
                return dict((jid,job) for (jid,job) in self.__jobs.items() if \
                    (   (not node or job['node']==node) or \
                        (not pool or job['pool']==pool) or \
                        (not ts or job['ts']>ts) )
                )
            except Exception as e: self.__logger.warning(e,exc_info=True)
        return None

    def sync(self,data={}):
        #update local state with incoming state if job not in local state or job ts >= local jobs ts
        #return updated items
        with self.__lock:
            updated={}
            try:
                for jid,job in data.items():
                    ts=job['ts']
                    if jid not in self.__jobs or self.__jobs[jid]['ts'] <= ts: 
                        self.__logger.debug ('updating %s %s'%(jid,job))
                        self.__jobs[jid].update(job)
                        updated[jid]=self.__jobs[jid]
            except Exception as e: self.__logging.warning(e,exc_info=True)
        return updated

    def expire(self):
        #delete jobs from state if they have not been updated in expire time
        with self.__lock:
            try:
                for jid,job in self.__jobs.copy().items():
                    if time.time()-job['ts'] > self.__expire: 
                        self.__logger.debug('expired %s %s'%(jid,job))
                        del self.__jobs[jid]
            except Exception as e: self.__logger.warning(e,exc_info=True)

    def get(self,jid):
        #get job by ID
        with self.__lock:
            try:
                return self.__jobs.get(jid)
            except Exception as e: self.__logger.warning(e,exc_info=True)
    
    def update(self,jid,data):
        #get job by ID
        with self.__lock:
            try:
                if jid in self.__jobs: 
                    self.__jobs[jid].update(data)
                    return self.__jobs.get(jid)
                else: return False
            except Exception as e: self.__logger.warning(e,exc_info=True)

    def new(self,pool=None,node=None,**jobargs):
        #add a new job to the state
        with self.__lock:
            try:
                if not pool: return False #jobs have to have a pool to run in
                jid=str(uuid.uuid1())
                self.__jobs[jid]={  ts:time.time(), 
                                    'pool':pool, 
                                    'node':node,
                                    'state':'new' }
                self.__jobs[jid].update(jobargs)
                return jid
            except Exception as e: self.__logger.warning(e,exc_info=True)

    def run(self):
        self.__logger.info('started')
        while not self.shutdown.is_set():
            time.sleep(1)
            self.expire()


class Node(threading.Thread):
    '''node poller/state sync thread
    initially we try to push all state to the node (sync_ts of 0)'''
    def __init__(self,name,state,sync=1,address=None,port=int('c137',16),timeout=10):
        self.__name=name
        self.__state=state
        threading.Thread.__init__(self,daemon=True,name='Node.'+self.__name)
        self.__logger=logging.getLogger(self.name)
        self.__address=address
        if not self.__address: self.__address=name
        self.__port=port
        self.__timeout=timeout
        self.__sync=sync
        self.__socket=None
        self.shutdown=threading.Event()
        self.start()

    def __sr(self,requests):
        #send/recieve request/response
        if not self.__socket:
                self.__logger.debug('connecting to %s:%s'%(self.__address,self.__port))
                try: 
                    self.__socket=socket.create_connection((self.__address,self.__port),timeout=self.__timeout)
                    self.__socket
                except Exception as e:
                    self.__logger.debug("%s:%s %s"%(self.__address,self.__port,e))
        if self.__socket:
            try:
                self.__socket.sendall(json.dumps(requests).encode())
                self.__socket.sendall('\n'.encode())
                l=''
                while True:
                    l+=self.__socket.recv(65535).decode()
                    if '\n' in l: return json.loads(l)
            except Exception as e: 
                self.__logger.warning(e,exc_info=True)
                if self.__socket: 
                    self.__socket.close()
                    self.__socket=None


    def run(self):
        ts=None #timestamp of the last sync
        while not self.shutdown.is_set():
            time.sleep(self.__sync)
            #dump all jobs for this node updated more recently than the last sync
            request={'stats':{},'sync':self.__state.dump(node=self.__name,ts=ts)}
            if ts: request.update(ts=ts)
            ts=time.time()
            self.__logger.debug('sent %s'%request)
            responses=self.__sr([request])
            if responses:
                response=responses[0]
                self.__logger.debug('got %s'%request)
                if 'sync' in response:
                    self.__state.sync(response['sync'])
                if 'state' in response: pass

        if self.__socket:self.__socket.close()


class Pool(threading.Thread):

    def __init__(self,name,pool,state):
        self.__name=name
        self.__pool=pool
        self.__state=state
        threading.Thread.__init__(self,daemon=True,name='Pool.'+self.__pool)
        self.__logger=logging.getLogger(self.name)
        self.shutdown=threading.Event()
        self.start()

    def run(self): pass



class Main:
    '''meeseeks box main thread'''
    
    def __init__(self,nodes={},pools={},**kwargs):

        #get our nodename
        self.name=kwargs.get('name',socket.gethostname())

        #set up logger
        self.__logger=logging.getLogger(self.name+'.main')

        #start the request server
        listen=kwargs.get('listen',{})        
        self.__listener=RequestListener( (  listen.get('address','localhost'),
                                            listen.get('port',int('c137',16))   ), 
                            RequestHandler)
        self.__listener.kwargs=listen #for passing ssl params and other options
        self.__listener.handler=self
        self.__listener.server_thread=threading.Thread(target=self.__listener.serve_forever)
        self.__listener.server_thread.daemon=True
        self.__listener.server_thread.start()

        #init state
        self.state=State(name=self.name,**kwargs.get('state',{}))

        #init pools (local job queues)
        self.pools={} #pools we process jobs for
        for p,cfg in pools.items():
            self.__logger.info('creating pool %s %s'%(p,cfg))
            #self.pools[p]=Pool(p,self.state,**cfg)

        #init nodes
        self.nodes={} #nodes we sync
        for n,cfg in nodes.items():
            self.__logger.info('adding node %s %s'%(n,cfg))
            self.nodes[n]=Node(n,self.state,**cfg)

    #handle incoming requests
    def handle(self,request):
        self.__logger.debug(request)
        response={}
        #we're being pushed state from upstream node and should return ours
        if 'sync' in request:
            #sync incoming state
            self.state.sync(request['sync'])
            #reply with anything newer than the upstream ts
            response['sync']=self.state.dump(ts=request.get('ts'))
        #submit job
        if 'submit' in request: 
            response['submit']={}
        #query job
        if 'query' in request:
            response['query']={}
        #kill job
        if 'kill' in request:
            response['kill']={}
        #get node stats
        if 'stats' in request:
            response['stats']={}
        return response


    def run(self):
        self.__logger.info('node %s running'%self.name)

        while True:
            try:
                time.sleep(1)


            except KeyboardInterrupt: break
            except Exception as e: 
                self.__logger.error(e,exc_info=True)

        #existence is pain!
        self.__logger.warning('shutting down')
        try:
            self.__listener.shutdown()
            self.__listener.server_thread.join()
        except Exception as e: self.__logger.error(e,exc_info=True)

        self.state.shutdown.set()
        self.state.join()

        for node in self.nodes.values():
            node.shutdown.set()
            node.join()

        for pool in self.pools.values():
            pool.shutdown.set()
            pool.join()

        return True
            


if __name__ == '__main__':

    cfg=dict(nodes={},pools={})
    if len(sys.argv)>1:
        for f in sys.argv[1:]: 
            try:
                with open(f) as cfh:
                    for l in cfh.readlines():
                        try: 
                            if l.startswith('log'):
                                if '=' in l: #can be a basic key=vlue (level=DEBUG...)
                                    log_config=parse_kvs(parse_line(l)[1:])
                                    if 'level' in log_config: log_config['level']=eval('logging.'+log_config['level']) #python 2.6 compatible
                                    logging.basicConfig(**log_config)
                                else: #or a full dictionary for loading modules, etc..
                                    log_config=eval(' '.join(parse_line(l)[1:]))
                                    log_config.update(version=1)
                                    logging.config.dictConfig(log_config)
                            l=parse_line(l)
                            if len(l)<2: continue
                            if l[0]=='node': #node <hostname> [args]
                                cfg['nodes'][l[1]]=parse_kvs(l[2:])
                            if l[0]=='pool': #pool <poolname> [args]
                                cfg['pools'][l[1]]=parse_kvs(l[2:])
                            else: #<config-key> [values]
                                cfg[l[0]]=parse_kvs(l[1:])
                        except Exception as e: logging.warning(e,exc_info=True)
            except Exception as e: logging.warning(e,exc_info=True)
    
    m=Main(**cfg)
    sys.exit(m.run())
