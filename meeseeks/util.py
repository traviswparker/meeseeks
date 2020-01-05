#!/usr/bin/env python3

import sys
import json
import ssl
import importlib

def import_plugin(plugin):
    '''imports by path.module.Plugin and returns the plugin class'''
    m=importlib.import_module('.'.join(plugin.split('.')[:-1]))
    try: attrlist = m.__all__
    except AttributeError: attrlist = dir (m)
    except Exception as e:
        print (e,file=sys.stderr)
        return None
    return getattr (m,plugin.split('.')[-1])

def merge(a, b):
    '''merges b into a if possible, replacing non-dict values
        !key in b will delete key from a'''
    a=a.copy() #operate on copy of dest dict so we can delete keys
    for key in sorted(b): #process !keys before others
        if str(key).startswith('!'): #delete key
            if key in a: del a[key[1:]]
            continue
        elif key in a: #update
            #recurse into nested
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                a[key]=merge(a[key], b[key])
            #replace values
            else: a[key]=b[key]
        #else add key
        else: a[key] = b[key]
    return a #return merged dict

def read_cfg_files(args):
    cfg={}
    if type(args) is not list: args=[args]
    for f in args: 
        try:
            with open(f) as fh: cfg=merge(cfg,json.load(fh))
        except Exception as e:
            print (e,file=sys.stderr)
    return cfg

def cmdline_parser(args):
    #parse args
    # cfg={key[.subkey.s]=value[,value..] args preceeding first non = argument}
    cfg={}
    i=0
    for arg in args:
        if '=' in arg:
            k,v=arg.split('=',1)
            c=cfg #may be sub dict, for k.s=v arguments
            while '.' in k: #walk subkeys
                k,sk=k.split('.',1)
                c,k=c.setdefault(k,{}),sk
            if v.isnumeric(): v=int(v)
            elif ',' in v: v=list(v.split(','))
            elif not v: v={}
            c[k]=v
        else: break #stop at first arg without =
        i+=1
    args=args[i:]
    return cfg,args

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
