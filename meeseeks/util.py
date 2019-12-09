#!/usr/bin/env python3

import sys
import json

def read_cfg_files(args):
    cfg={}
    if type(args) is not list: args=[args]
    for f in args: 
        try:
            with open(f) as fh: cfg.update(json.load(fh))
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