#!/usr/bin/env python3

import sys
import logging
import signal
import os
import time
import threading
import json 
import fnmatch

from .job import Job

class Watch(threading.Thread):
    '''meeseeks-watch file watcher thread'''
    def __init__(self,name='watch',client=None,**cfg):
        self.client=client
        self.__jobs={} #map of index_filename to job object
        self.__files={} #map of glob -> files matching
        self.__cache=[] #cached DirEntries
        self.cfg={}
        self.config(**cfg)
        self.cfg.update(name=name)
        self.logger=logging.getLogger(name)
        self.shutdown=threading.Event()
        threading.Thread.__init__(self,name=name)
        self.start()

    def config(self,**cfg): 
        self.cfg.update(cfg)
        self.path=self.cfg.get('path')
        self.refresh=int(cfg.get('refresh',10))
        self.rescan=int(cfg.get('rescan',60))/self.refresh

    def set_file_status(self,index,filename,job):
        '''set the file processing status and last modified time
        override this in a subclass to store status a different way'''
        self.logger.debug('set %s %s %s'%(index,filename,job))
        with open(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,filename,job['state'])),'w') as fp: 
            json.dump(job,fp,sort_keys=True,indent=4)
        if self.cfg.get('updated'): #save mtime
            try:
                with open(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,filename,'mtime')),'w') as fp:
                    fp.writelines((str(os.stat(os.path.join(self.path,filename)).st_mtime)))
            except: pass

    def check_file_status(self,index,file,status='done'):
        '''check the file status
        override this in a subclass to store status a different way
        should return True if the status=status, False if not'''
        if os.path.exists(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,file.name,status))): 
            if not self.cfg.get('updated'): return True #is done
            #check file mtime against saved mtime
            try:
                with open(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,file.name,'mtime'))) as fp:
                    mtime=float(fp.readline())
                if mtime != file.stat().st_mtime: return False #file has been updated
            except: pass #no saved mtime, consider done
            return True
        return False #not processed

    def start_job(self,index,subindex=0,file=None,fparts=[],**kwargs):
        #index is jobs index
        #subindex is jobspec list index (usually 0)
        #file is filename, fparts is filename as parts
        jobspec=self.cfg['jobs'][index] #get jobspec(s) at index
        if jobspec:
            if type(jobspec) is not list: jobspec=[jobspec] #make single jobspec a list
            if file: self.logger.info('starting job %s_%s'%(index,file.name))
            else: self.logger.info('starting job %s'%index)
            subindex=min(subindex,len(jobspec)-1) #get matching or highest jobspec
            jobspec=jobspec[subindex]
            if jobspec:
                jobspec=jobspec.copy() #don't modify configured spec
                c=self.cfg.copy() #get watch config
                c.update(**kwargs) #get extra jobargs
                #add in file
                jid=str(index)
                c.update(index=jid)
                if file: 
                    c.update(filename=file.name,file=file.path)
                    #add in filename parts
                    c.update((str(i),p) for (i,p) in enumerate(fparts))
                    #add in fileset 
                    if 'fileset' in kwargs: 
                        c.update(('fileset'+str(i),f) for (i,f) in enumerate(kwargs['fileset']))
                    jid+='_'+file.name
                #do format string subs
                for k,v in jobspec.items():
                    if type(v) is str: jobspec[k]=v%c
                    elif type(v) is list: jobspec[k]=[e%c for e in v]
                self.logger.debug(jobspec)
                job=Job(client=self.client,**jobspec)
                if job.start(): 
                    self.__jobs[jid]=job
                    return True
                else: 
                    self.logger.error(job)
                    return False
        else: index=0 #use index 0 if no jobspec so file can be marked
        #no job started
        if file: self.set_file_status(self,index,file,{'state':'done','skipped':True}) #no job, mark file as skipped

    def rescan_path(self):
        #rescans path
        self.logger.debug('rescanning %s'%(self.path))
        minage,maxage,get_mtime=self.cfg.get('min_age'),self.cfg.get('max_age'),self.cfg.get('updated')
        files=[]
        for file in os.scandir(self.path):
            try:
                if file.name.startswith('.'): continue
                if minage or maxage or get_mtime: 
                    #get and cache the mtimes
                    age=time.time()-file.stat().st_mtime
                    if (minage and age < minage) or (maxage and age > maxage): continue
                files.append(file)
            except Exception as e: self.logger.warning('%s %s'%(file.name,e)) #can't access?
        self.__cache=sorted(files,key=lambda file:file.name,reverse=(not self.cfg.get('reverse',False)))
        return len(self.__cache)

    def run(self):
        self.__files={}
        
        rescan_count=-1
        while not self.shutdown.is_set():

            match=self.cfg.get('fileset')
            globs=self.cfg.get('glob')
            jobs=self.cfg.get('jobs',[])
            if globs and type(globs) is not list: globs=[globs]
            split=self.cfg.get('split')
            max_index=self.cfg.get('max_index')

            #clean up jobs first
            for jid,job in self.__jobs.copy().items():
                if job.poll(): #a job exited
                    if job.multi: #this is a multi-node job, we don't have a single state
                        #if any job failed, kill and restart the multi-job
                        if any(j.get('state')=='failed' for j in job.poll().values()): 
                            self.logger.warning('job %s failed'%jid)
                            job.kill() #stop the remaining parts so it can be restarted
                    elif job.state != 'done': self.logger.warning('job %s %s'%(jid,job.state))
                    if not job.is_alive():
                        if '_' in jid: #set file status if this job is for a file
                            index,filename=jid.split('_',2)
                            self.set_file_status(index,filename,job.poll())
                        del self.__jobs[jid]

            if globs: #if we are tracking files
                rescan_count=(rescan_count+1) % self.rescan

                #rescan files, start jobs on unprocessed files
                if not rescan_count: #scan files
                    try: 
                        n=self.rescan_path()
                        self.logger.debug('%s: %s files'%(self.path,n))
                    except Exception as e: self.logger.error(e,exc_info=True)

                    #build lists
                    for glob in globs:
                        self.__files[glob]=[file for file in self.__cache if fnmatch.fnmatch(file.name,glob)]
                        self.logger.debug('%s: %s files'%(glob,len(self.__files[glob])))

                    #check files and start jobs
                    for list_index,glob in enumerate(globs): #list index (index of glob->file list)
                        if split and match and list_index: break #if fileset, only use the first list
                        for file_index,file in enumerate(self.__files[glob]): #index down file list
                            if max_index and file_index > max_index: break
                            job_index=min(file_index,len(jobs)-1) #use matching or last job index available
                            #check job 0 to file_index for run_all, just file_ index if not
                            if self.cfg.get('run_all',True): min_index=0
                            else: min_index=job_index
                            for index in range(min_index,job_index+1): 
                                start,jobargs=False,{}
                                if not jobs[index]: continue #no job for this index, skip it.
                                #check file status
                                try: 
                                    #job for this file is still running, don't start the next one
                                    if '%s_%s'%(index,file.name) in self.__jobs: break 
                                    #is file done (and not updated if updated=1)?
                                    status=self.check_file_status(index,file) 
                                    #if not retrying, failed=done
                                    if not status and not self.cfg.get('retry',True): 
                                        status=self.check_file_status(index,file,'failed')
                                except Exception as e:
                                    self.logger.warning("%s %s"%(file.name,e))
                                    status=True #skip this one it smells funny

                                if not status: #if file is not running and not processed
                                    fparts=file.name.split(split) #get filename parts
                                    #if fileset mode
                                    if split and match:
                                        mpat=split.join(fparts[:match]) #generate match string
                                        fileset={} #files across lists that match 
                                        for g,files in self.__files.items(): #check across lists
                                            for f in files:
                                                if f.name.startswith(mpat):
                                                    fileset[g]=f #found it
                                                    break
                                        logging.debug('fileset %s matching %s %s'%(index,mpat,fileset))
                                        if len(fileset) == len(globs): #complete fileset
                                            jobargs.update(fileset=[ fileset[g].name for g in globs ])
                                            self.logger.debug(jobargs)
                                            start=True
                                    #not fileset
                                    else: start=True
                                #start job
                                if start:
                                    try: self.start_job(index,list_index,file,fparts,**jobargs)
                                    except Exception as e: self.logger.error(e)
                                    break #if run_all, don't start the next index job until this one is finished
            
            else: #just track job
                for index in range(len(jobs)):
                    if str(index) in self.__jobs: continue #still running
                    self.start_job(index) #start it

            c=0
            while not self.shutdown.is_set():
                c+=1
                time.sleep(1)
                if c > self.refresh: break
        
        for jid,job in self.__jobs.items(): 
            self.logger.info('killing job %s'%jid)
            job.kill()