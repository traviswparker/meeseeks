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
        self.__jobs={} #map of jid to [file,job object]
        self.__files={} #map of glob -> files matching
        self.__dir=[] #cached DirEntries
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
        self.logger.debug('set %s %s %s'%(index,file.name,job))
        with open(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,file.name,job['state'])),'w') as fp: 
            json.dump(job,fp,sort_keys=True,indent=4)
        if self.cfg.get('updated'): #save mtime
            try:
                with open(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,file.name,'mtime')),'w') as fp:
                    fp.writelines((str(file.stat().st_mtime)))
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
                    c.update(filename=file.name,file=file.path)
                    #add in filename parts
                    c.update((str(i),p) for (i,p) in enumerate(fparts))
                    #do format string subs
                    for k,v in jobspec.items():
                        if type(v) is str: jobspec[k]=v%c
                        elif type(v) is list: jobspec[k]=[e%c for e in v]
                    self.logger.debug(jobspec)
                    job=Job(client=self.client,**jobspec)
                    if job.start(): 
                        self.__jobs['%s_%s'%(index,file.name)]=job
                        return True
                    else: 
                        self.logger.warning(job)
                        return False
        else: index=0 #use index 0 if no jobspecs so file can be marked
        #no job started
        self.set_file_status(self,index,file,{'state':'done','skipped':True}) #no job, mark file as skipped

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
            except Exception as e: self.logger.debug(e) #can't access?
        self.__dir=sorted(files,key=lambda file:file.name,reverse=(not self.cfg.get('reverse',False)))
        return len(self.__dir)

    def run(self):
        match=self.cfg.get('fileset')
        globs=self.cfg.get('glob')
        if type(globs) is not list: globs=[globs]
        self.__files={}
        split=self.cfg.get('split')
        max_index=self.cfg.get('max_index')

        rescan_count=-1
        while not self.shutdown.is_set():
            
            #clean up jobs first
            for index_file,job in self.__jobs.copy().items():
                if not job.is_alive(): #job exited
                    index,file=index_file.split('_',1)
                    self.logger.info('job[%s] for %s %s'%(index,file,job.state))
                    self.set_file_status(index,file,job.poll())
                    del self.__jobs[index_file]

            rescan_count=(rescan_count+1) % self.rescan

            #rescan files, start jobs on unprocessed files
            if not rescan_count: #scan files
                try: self.rescan_path()
                except Exception as e: self.logger.error(e,exc_info=True)

                #build lists
                for glob in globs:
                    self.__files[glob]=[file for file in self.__dir if fnmatch.fnmatch(file.name,glob)]
                    self.logger.debug('%s: %s files'%(glob,len(self.__files[glob])))

                #check files and start jobs
                for lindex,glob in enumerate(globs): #list index (index of glob->file list)
                    for file_index,file in enumerate(self.__files[glob]): #index down file list
                        if max_index and file_index > max_index: break
                        job_index=min(file_index,len(self.cfg.get('jobs',[]))-1) #at last jobspec, use last index available
                        if self.cfg.get('run_all',True): min_index=0
                        else: min_index=job_index
                        for index in range(min_index,job_index+1): #0 -> index for run_all, index -> index if not
                            if not self.cfg['jobs'][index]: continue #no job for this index, skip it.
                            try: 
                                #job for this file is still running, don't start the next one
                                if '%s_%s'%(index,file.name) in self.__jobs: continue 
                                status=self.check_file_status(index,file) #is file processed
                                if not status and not self.cfg.get('retry',True): #if not retrying, failed=processed
                                    status=self.check_file_status(index,file,'failed')
                            except Exception as e:
                                self.logger.warning(e,exc_info=True)
                                status=True #skip this

                            if not status: #if file is not running and not processed
                                fparts=file.name.split(split)
                                if split and match: #if we're matching parts to make a set
                                    mpat=split.join(fparts[:match]) #generate match string
                                    logging.debug('fileset: match %s'%mpat)
                                    fileset={} #files across lists that match 
                                    for glob,files in self.__files.items(): #check across lists
                                        for file in files:
                                            if file.name.startswith(mpat):
                                                fileset[glob]=file #found it
                                                break
                                    if len(fileset) == len(globs): #complete fileset
                                        self.logger.info('starting jobs[%s] for fileset %s'%(index,list(fileset.values())))
                                        for lindex,glob in enumerate(globs): #start a job for each file in set
                                            try: self.start_file_job(index,lindex,fileset[glob],fileset[glob].split(split))
                                            except Exception as e: self.logger.error(e,exc_info=True)
                                        break #if run_all, don't start the next index job until this one is finished
                                else: #not fileset, start job
                                    try: self.start_file_job(index,lindex,file,fparts)
                                    except Exception as e: self.logger.error(e,exc_info=True)
                                    break #if run_all, don't start the next index job until this one is finished

            c=0
            while not self.shutdown.is_set():
                c+=1
                time.sleep(1)
                if c > self.refresh: break
        
        for index_file,job in self.__jobs.items(): 
            self.logger.warning('killing job for %s'%index_file)
            job.kill()
