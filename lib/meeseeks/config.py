#!/usr/bin/env python3 

import json

class Config(object):
    '''config key-value store, supports merging on update
    and addressing subkeys using [key.subkey.subkey] format
    dict-like access using []/get/setdefault/keys/values/items/iteration is available
        (to get the store reflected as a dict use self.copy())
    self.merge(A,B) will merge B into A and return the result dict, see merge() docs
        (to merge using this object use self.merge(self,B | A,self))
    self.update(B) will merge B into this object
    self.dump()/self.load(str) to go to/from JSON
    self.resolve() will resolve %(key)s format strings in values to the value of key if possible
    resolve is performed on update by default, set autoresolve=False to disable this
    '''
    def __init__(self,*args,**kwargs):
        self.autoresolve=True #set false to not resolve vars on update
        self.__unresolved=0 #unresolved variable count
        self.__cfg={}
        self.update(*args,**kwargs)
    
    #miscellaneous dict methods
    def __len__(self): return len(self.__cfg) 
    def __str__(self): return str(self.__cfg)
    def __repr__(self): return repr(self.__cfg)
    def clear(self): return self.__cfg.clear()
    #a copy of Config will be a copy of the internal dict
    def copy(self): return self.__cfg.copy()
    #these all operate on a copy of the private dict
    def __iter__(self): return self.copy().__iter__()
    def items(self): return self.copy().items()
    def keys(self): return self.copy().keys()
    def values(self): return self.copy().values()

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

    def __getitem__(self,k):
        '''gets dotted-key k
        example: if k is 'a.b.c', will return d['a']['b']['c']'''
        d=self.__cfg
        kk=k.split('.')
        while kk:
            k=kk.pop(0)
            d=d[k]
        return d

    def __getattr__(self, name): 
        '''access first-level config keys as attributes'''
        return self.get(name)

    def __contains__(self,k):
        '''checks if dotted-key k exists'''
        try: 
            self[k]
            return True
        except KeyError: return False

    def get(self,k,default=None):
        '''gets dotted-key k, returns default if not found'''
        try: return self[k]
        except KeyError: return default

    def setdefault(self,k,default=None):
        '''get dotted-key k, sets default of not found'''
        try: return self[k]
        except KeyError: 
            self[k]=default
            return self[k]

    def update(self,*args,**kwargs):
        '''merge in updates from dict(s)/Config objects, or kwargs
        see merge() for !/+/- key prefixes
        dotted-keys in kwargs will replace key in A, not merge
        if autoresolve=True, resolves vars and and returns count of unresolved''' 
        c=self 
        #merge will return a dict
        for arg in args: c=self.merge(c,arg)
        if kwargs: c=self.merge(c,kwargs)
        self.__cfg=c.copy() #set internal dict to merged dict
        if self.autoresolve: return self.resolve()

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

    #config merging
    def merge(self, a, b):
        '''merges b into a if possible
            a/b should be dict-like, including Config instances
            dicts will be merged, non-dict values will be replaced
            !key in b will delete key from a
            +key in b will append b[key] list to a[key] list (a+b)
            key+ in b will prepend b[key] list to a[key] list (b+a)
            -key in b will remove b[key] items from a[key] list (a-b)'''
        a=a.copy() #operate on copy of dest dict so we can delete keys
        for key in sorted(b): #process !keys before others
            append_list=prepend_list=diff_list=False
            bkey=key #bkey is the original key in b, key will change if list operation 
            if str(key).startswith('!'): #delete key
                if key[1:] in a: del a[key[1:]]
                continue
            elif str(key).startswith('+'): #append list
                append_list=True
                bkey,key=key,key[1:]
            elif str(key).endswith('+'): #prepend list
                prepend_list=True
                bkey,ke=key,key[:-1]
            elif str(key).startswith('-'): #diff list
                diff_list=True
                bkey,key=key,key[1:]
            #merge by recursing into nested
            if key in a and isinstance(a[key], dict) and isinstance(b[key], dict):
                a[key]=self.merge(a[key], b[key])
            #+/- list operations
            elif key in a and append_list and ( isinstance(a[key], list) and isinstance(b[bkey], list) ):
                a[key]=a[key]+b[bkey]
            elif key in a and prepend_list and ( isinstance(a[key], list) and isinstance(b[bkey], list) ):
                a[key]=b[bkey]+a[key]
            elif key in a and diff_list and ( isinstance(a[key], list) and isinstance(b[bkey], list) ):
                a[key]=[i for i in a[key] if i not in b[bkey]]
            #replace values as long we are not subtracting values
            elif not diff_list: a[key] = b[bkey]
        return a #return merged dict
    
    #%(key)s resolution
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

    #to/from JSON
    import json
    def dump(self,**kwargs): 
        '''dump config as JSON'''
        return json.dumps(self.__cfg,**kwargs)
    def load(self,j,**kwargs):
        '''load config from JSON'''
        return self.update(json.loads(j,**kwargs))
