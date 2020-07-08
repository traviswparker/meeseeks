#!/usr/bin/env python3

import sys
import os
import pwd, grp
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

def merge(a, b, merge_lists=False):
    '''merges b into a if possible, replacing non-dict values
        if merge_lists, list value in b will be appended to list in a
        !key in b will delete key from a'''
    a=a.copy() #operate on copy of dest dict so we can delete keys
    for key in sorted(b): #process !keys before others
        if str(key).startswith('!'): #delete key
            if key[1:] in a: del a[key[1:]]
            continue
        elif key in a: #update
            #recurse into nested
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                a[key]=merge(a[key], b[key])
            #merge lists
            elif merge_lists and isinstance(a[key], list) and isinstance(b[key], list):
                a[key].extend(b[key])
            #replace values
            else: a[key] = b[key]
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