#!/usr/bin/env python3

import sys
import logging
import signal
import os
import time
import threading
import json 
import glob

from .job import Job

class Watch(threading.Thread):
    '''meeseeks-watch file watcher thread'''
    def __init__(self,name,client=None,**cfg):
        self.client=client
        self.jobs={} #map of jid to [file,job object]
        self.files={} #map of glob -> files matching
        self.cfg=cfg
        self.cfg.update(name=name)
        self.path=self.cfg['path']
        self.logger=logging.getLogger(name)
        self.shutdown=threading.Event()
        threading.Thread.__init__(self,name=name)
        self.refresh=int(cfg.get('refresh',10))
        self.rescan=int(cfg.get('rescan',60))/self.refresh
        self.start()

    def set_file_status(self,index,file,job):
        '''set the file processing status and last modified time
        override this in a subclass to store status a different way'''
        self.logger.debug('set %s %s %s'%(index,file,job))
        with open(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,file,job['state'])),'w') as fp: 
            json.dump(job,fp,sort_keys=True,indent=4)
        if self.cfg.get('updated'): #save mtime
            try:
                with open(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,file,'mtime')),'w') as fp:
                    fp.writelines((str(os.stat(file).st_mtime)))
            except: pass

    def check_file_status(self,index,file,status='done'):
        '''check the file status
        override this in a subclass to store status a different way
        should return True if the status=status, False otherwise'''
        if '%s_%s'%(index,file) in self.jobs: return True #is running
        elif os.path.exists(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,file,status))): 
            if not self.cfg.get('updated'): return True #is done
            #check file mtime against saved mtime
            try:
                with open(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,file,'mtime'))) as fp:
                    mtime=float(fp.readline())
                if mtime != os.stat(file).st_mtime: return False #file has been updated
            except: pass #no saved mtime, consider done
            return True
        return False #not processed

    def start_file_job(self,index,lindex,file,fparts):
        #index is jobspec  index
        #lindex is jobspec list index (usually 0)
        #file is filename, fparts is filename as parts
        jobspec=self.cfg['jobs'][index] #get jobspec(s) at index
        if jobspec:
            if type(jobspec) is not list: jobspec=[jobspec] #make single jobspec a list
            self.logger.info('starting job[%s,%s] for %s'%(index,lindex,file))
            if len(jobspec)>lindex: #jobspec for this lindex exists
                jobspec=jobspec[lindex]
                if jobspec:
                    jobspec=jobspec.copy() #don't modify configured spec
                    c=self.cfg.copy() #get watch config
                    #add in file
                    c.update(filename=file,file=os.path.join(self.path,file))
                    #add in filename parts
                    c.update((str(i),p) for (i,p) in enumerate(fparts))
                    #do format string subs
                    for k,v in jobspec.items():
                        if type(v) is str: jobspec[k]=v%c
                        elif type(v) is list: jobspec[k]=[e%c for e in v]
                    self.logger.debug(jobspec)
                    job=Job(client=self.client,**jobspec)
                    if job.start(): 
                        self.jobs['%s_%s'%(index,file)]=job
                        return True
                    else: 
                        self.logger.warning(job)
                        return False
        else: index=0 #use index 0 if no jobspecs so file can be marked
        #no job started
        self.set_file_status(self,index,file,{'state':'done','skipped':True}) #no job, mark file as skipped

    def rescan_files(self,g):
        #rescans path
        logging.debug('rescanning %s for %s'%(self.path,g))
        minage,maxage=self.cfg.get('min_age'),self.cfg.get('max_age')
        pathglob=os.path.join(self.path,g)
        files=[]
        fg=(os.path.split(f)[1] for f in glob.iglob(pathglob))
        #filter by age
        if minage or maxage:
            for f in fg:
                age=time.time()-os.stat(f).st_mtime
                if (minage and age < minage) or (maxage and age > maxage): continue
                files.append(f)
        else: files=[f for f in fg]
        #sort files descending by default
        self.files[g]=sorted(files,reverse=(not self.cfg.get('reverse',False)))
        return len(files)
        
    def run(self):
        match=self.cfg.get('match')
        globs=self.cfg.get('glob')
        if type(globs) is not list: globs=[globs]
        self.files={g:[] for g in globs}
        split=self.cfg.get('split')
        max_index=self.cfg.get('max_index')
        rescan_count=-1
        while not self.shutdown.is_set():
            
            #clean up jobs first
            for index_file,job in self.jobs.copy().items():
                if not job.is_alive(): #job exited
                    index,file=index_file.split('_',1)
                    self.logger.info('job[%s] for %s %s'%(index,file,job.state))
                    self.set_file_status(index,file,job.poll())
                    del self.jobs[index_file]

            #rescan files, start jobs on unprocessed files
            rescan_count=(rescan_count+1) % self.rescan
            if not rescan_count: #scan files
                for g in self.files.keys(): self.rescan_files(g)
                for lindex,g in enumerate(globs): #list index (index of glob->file list)
                    for index,file in enumerate(self.files[g]): #index down file list
                        if max_index and index > max_index: break
                        index=min(index,len(self.cfg.get('jobs',[]))-1) #at last jobspec, use last index available
                        try: 
                            status=self.check_file_status(index,file)
                            if not status and not self.cfg.get('retry',True):
                                status=self.check_file_status(index,file,'failed')
                        except Exception as e:
                            self.logger.warning(e,exc_info=True)
                            status=True #skip this
                        if not status: #if file is not running and not processed
                            fparts=file.split(split)
                            if split and match: #if we're matching parts to make a set
                                mpat=split.join(fparts[:match]) #generate match string
                                logging.debug('fileset: match %s'%mpat)
                                fileset={} #files across lists that match 
                                for g,files in self.files.items(): #check across lists
                                    for file in files:
                                        if file.startswith(mpat):
                                            fileset[g]=file #found it
                                            break
                                if len(fileset) == len(globs): #complete fileset
                                    self.logger.info('starting jobs[%s] for fileset %s'%(index,list(fileset.values())))
                                    for lindex,g in enumerate(globs): #start a job for each file in set
                                        try: self.start_file_job(index,lindex,fileset[g],fileset[g].split(split))
                                        except Exception as e: self.logger.error(e,exc_info=True)
                            else: 
                                try: self.start_file_job(index,lindex,file,fparts)
                                except Exception as e: self.logger.error(e,exc_info=True)
            c=0
            while not self.shutdown.is_set():
                c+=1
                time.sleep(1)
                if c > self.refresh: break
        
        for index_file,job in self.jobs.items(): 
            self.logger.warning('killing job for %s'%index_file)
            job.kill()
