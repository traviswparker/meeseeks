#!/usr/bin/env python3
import logging
import json
import socket
import ssl
import threading

class Client:

    def __init__(self,remote_node,address=None,port=13700,timeout=10,**cfg):
        self.remote_node=remote_node #node we connect to
        self.logger=logging.getLogger(self.remote_node)
        self.address=address
        if not self.address: self.address=self.node
        self.port=port
        self.timeout=timeout
        self.__socket=None
        self.__lock=threading.Lock()
        self.cfg=cfg

    def close(self): 
        if self.__socket: self.__socket.close()

    def connect(self):
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
        if self.__socket: return True

    def request(self,requests):
        #conect and send/recieve request/response
        with self.__lock:
            if self.connect():
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