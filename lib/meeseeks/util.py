#!/usr/bin/env python3

import sys
import os
import pwd, grp
import json
import ssl
import importlib

class Config(object):
    '''config key-value store, supports addressing subkeys using key.subkey.subkey format
    '''
    def __init__(self,*args,**kwargs):
        self.__cfg={}
        self.update(*args,**kwargs)
        self.__unresolved=0 #unresolved variable count

    def merge(self, a, b):
        '''merges b into a if possible, replacing non-dict values
            if merge_lists, list value in b will be appended to list in a
            !key in b will delete key from a
            +key in b will append b[key] to list in a[key]
            -key in b will remove b[key] items from list in a[key]'''
        a=a.copy() #operate on copy of dest dict so we can delete keys
        for key in sorted(b): #process !keys before others
            merge_lists=diff_lists=False
            if str(key).startswith('!'): #delete key
                if key[1:] in a: del a[key[1:]]
                continue
            elif str(key).startswith('+'): #append list
                merge_lists=True
                key=key[1:]
            elif str(key).startswith('-'): #diff list
                diff_lists=True
                key=key[1:]
            if key in a: #update
                #recurse into nested
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    a[key]=self.merge(a[key], b[key])
                #merge lists
                elif merge_lists and ( isinstance(a[key], list) and isinstance(b['+'+key], list) ):
                    a[key].extend(b['+'+key])
                elif diff_lists and ( isinstance(a[key], list) and isinstance(b['-'+key], list) ):
                    a[key]=[i for i in a[key] if i not in b['-'+key]]
                #replace values
                else: a[key] = b[key]
            #else add key
            else: a[key] = b[key]
        return a #return merged dict

    def __getitem__(self,k):
        d=self.__cfg
        kk=k.split('.')
        while kk:
            k=kk.pop(0)
            d=d[k]
        return d

    def __getattr__(self, name): 
        '''access first-level config keys as attributes
        dotted-keys are not supported here'''
        return self.get(name)

    def __contains__(self,k):
        try: 
            self.__getitem__(k)
            return True
        except KeyError: return False

    def get(self,k,default=None):
        try: return self.__getitem__(k)
        except KeyError: return default

    def __setitem__(self,k,v):
        '''sets dotted-key k to value v
        example: if k is 'a.b.c', will create d['a']['b']['c']'''
        d=self.__cfg
        kk=k.split('.')
        while kk:
            k=kk.pop(0)
            if kk: #if key parts remain
                #if next is not dict, make it a dict
                if not isinstance(d.get(k),dict): d[k]=dict()
                d=d[k] #get dict under this key
        d[k]=v #at last key, drop value here

    def update(self,*args,**kwargs):
        '''merge in updates from dict(s) or kwargs
        see Config.merge() for !/+/- key prefixes''' 
        for arg in args: self.__cfg=self.merge(self.__cfg,arg)
        if kwargs: self.__cfg=self.merge(self.__cfg,kwargs)

    def __delitem__(self,k):
        '''deletes dotted key k
        example: if k is 'a.b.c' will del ['a']['b']['c']'''
        d=self.__cfg
        kk=k.split('.')
        while kk:
            k=kk.pop(0)
            if not kk: 
                del d[k] #k is last key, so del k from this level
                return
            d=d[k]

    def __resolve(self,d):
        for k,v in d.items():
            if isinstance(v,dict): self.__resolve(v)
            elif isinstance(v,list):
                for i,e in enumerate(v):
                    try: v[i]=e % self
                    except KeyError: self.__unresolved+=1
                    except: pass
            else:
                try: d[k]=v % self
                except KeyError: self.__unresolved+=1
                except: pass

    def resolve(self):
        '''recursively resolves format-string references to keys in values
        for example: {'a': 'foo', 'b': '%(a)sbar', 'c': '%(b)sbaz'} will resolve to
                     {'a': 'foo', 'b': 'foobar', 'c': 'foobarbaz'}
        returns count of unresolved references'''
        unresolved=0
        while True:
            self.__unresolved=0
            self.__resolve(self.__cfg)
            if self.__unresolved == unresolved: break
            else: unresolved=self.__unresolved
        return unresolved

    def dump(self): 
        #dump config as dict
        return self.__cfg.copy()

def import_plugin(plugin):
    '''imports by path.module.Plugin and returns the plugin class'''
    m=importlib.import_module('.'.join(plugin.split('.')[:-1]))
    try: attrlist = m.__all__
    except AttributeError: attrlist = dir (m)
    except Exception as e:
        print (e,file=sys.stderr)
        return None
    return getattr (m,plugin.split('.')[-1])

def read_cfg_files(args):
    cfg=Config()
    if type(args) is not list: args=[args]
    for f in args: 
        try:
            with open(f) as fh: cfg.update(json.load(fh))
        except Exception as e:
            print (e,file=sys.stderr)
    return cfg.dump()

def cmdline_parser(args):
    #parse args
    # cfg={key[.subkey.s]=value[,value..] args preceeding first non = argument}
    cfg=Config()
    i=0
    for arg in args:
        if '=' in arg:
            k,v=arg.split('=',1)
            if v.isnumeric(): v=int(v)
            elif ',' in v: v=list(v.split(','))
            elif not v: v={}
            cfg.update({k:v})
        else: break #stop at first arg without =
        i+=1
    args=args[i:]
    return cfg.dump(),args

def create_ssl_context(cfg):
    ssl_context=ssl.SSLContext(
        ssl.PROTOCOL_TLS,
        capath=cfg.get('capath'),
        cafile=cfg.get('cafile'))
    if 'ciphers' in cfg: ssl_context.set_ciphers(cfg['ciphers'])
    if 'options' in cfg: ssl_context.options|=cfg['options']
    if 'verify' in cfg: ssl_context.verify_mode=cfg['verify']
    if 'cert' in cfg: ssl_context.load_cert_chain(
        cfg.get('cert'),
        keyfile=cfg.get('key'),
        password=cfg.get('pass') )
    return ssl_context

def su(uid=None,gid=None,sub=False):
    #set effective or subprocess user/group if valid and not root,
    #if gid not provided, will use effective user's group
    #if uid is a string, get the uid
    #if sub=True, return a preexeec function that will set the uid/gid
    if type(uid) is str: uid=pwd.getpwnam(uid).pw_uid
    #if uid valid
    if uid and uid>0: 
        if type(gid) is str: gid=grp.getgrnam(gid).gr_gid
        #if no valid group specified, use user's group
        if gid and gid>0: pass
        else: gid=pwd.getpwuid(uid).pw_gid
        #reset effective uid (likely back to root) so we can change it again
        if sub:
            def preexec_fn():
                os.seteuid(os.getuid())
                os.setgid(gid)
                os.setuid(uid)
            return preexec_fn
        else:
            os.seteuid(os.getuid())
            os.setegid(gid)
            os.seteuid(uid)
            return os.getresuid(),os.getresgid()

#Config class tests
if __name__ == '__main__':
    c=Config()
    for f in sys.argv[1:]:
        with open(f) as h:
            c.update(json.load(h))
            print (f,c.resolve())
            print(json.dumps(c.dump(),sort_keys=True,indent=4))
    print ('k1' in c)
    print ('k9' in c)
    try: print (c['k1'])
    except Exception as e: print (e)
    print (c.k1)
    print (c.k9)
    c['k9.s1']='subkey1'
    print ('k9.s1' in c)
    del c['k9.s1']
    print (c)