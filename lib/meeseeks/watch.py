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
from .util import *
from .config import Config

class Watch(threading.Thread):
    '''meeseeks-watch file watcher thread'''
    def __init__(self,name='watch',client=None,**cfg):
        self.client=client
        self.__jobs={} #map of index_filename to job object
        self.__files={} #map of glob -> files matching
        self.__cache=[] #cached DirEntries
        self.cfg=Config()
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

    def set_file_status(self,index,filename,job=None,status=None):
        '''set the file processing status and last modified time
        override this in a subclass to store status a different way'''
        if job: status=job['state']
        self.logger.debug('set %s %s %s',index,filename,status)
        with open(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,filename,status)),'w') as fp: 
            if job: json.dump(job,fp,sort_keys=True,indent=4)
        if self.cfg.get('updated'): #save mtime
            with open(os.path.join(self.path,'._%s_%s_%s.%s'%(self.name,index,filename,'mtime')),'w') as fp:
                fp.writelines((str(os.stat(os.path.join(self.path,filename)).st_mtime)))

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

    def check_file_skip(self,mpat,split,skip):
        '''check if we skip this file'''
        skip_file=split.join([mpat,skip])
        self.logger.debug('%s skip if %s',mpat,os.path.join(self.path,skip_file))
        if os.path.exists(os.path.join(self.path,skip_file)): return skip_file
        return False

    def start_job(self,index,subindex=0,file=None,fparts=[],**kwargs):
        #index is jobs index
        #subindex is jobspec list index (usually 0)
        #file is filename, fparts is filename as parts
        jobspec=self.cfg['jobs'][index] #get jobspec(s) at index
        if jobspec:
            if type(jobspec) is not list: jobspec=[jobspec] #make single jobspec a list
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
                    c.update(('fileset'+str(i),f) for (i,f) in enumerate(kwargs['fileset']))
                    jid+='_'+file.name
                #do format string subs
                for k,v in jobspec.items():
                    if type(v) is str: jobspec[k]=v%c
                    elif type(v) is list: jobspec[k]=[e%c for e in v]
                self.logger.debug(jobspec)
                if file: self.logger.info('submit %s:%s %s for %s',index,subindex,jobspec.get('tags'),file.name)
                else: self.logger.info('submit %s:%s %s',index,subindex,jobspec.get('tags'))
                job=Job(client=self.client,**jobspec)
                if job.start(): 
                    self.__jobs[jid]=job
                    return True
                else: 
                    self.logger.warning('submit %s:%s %s failed',index,subindex,jobspec.get('tags'))
                    return False
        else: index=0 #use index 0 if no jobspec so file can be marked
        #no job started
        if file: self.set_file_status(self,index,file,{'state':'done','skipped':True}) #no job, mark file as skipped

    def rescan_path(self):
        #rescans path
        self.logger.debug('rescanning %s',self.path)
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
            except Exception as e: self.logger.warning('%s %s',file.name,e) #can't access?
        self.__cache=sorted(files,key=lambda file:file.name,reverse=(not self.cfg.get('reverse',False)))
        return len(self.__cache)

    def run(self):
        self.__files={}
        
        rescan_count=-1
        job_count=0
        try:
            while not self.shutdown.is_set():

                globs=self.cfg.get('glob')
                jobs=self.cfg.get('jobs',[])
                if globs and type(globs) is not list: globs=[globs]
                split=self.cfg.get('split')
                match=self.cfg.get('match')
                skip=self.cfg.get('skip')
                partial=self.cfg.get('partial')
                max_index=self.cfg.get('max_index')
                max_count=int(self.cfg.get('count',0))


                #clean up jobs first
                done=None #True if jobs done and False if a failure
                for jid,job in self.__jobs.copy().items():
                    if job.poll(): #a job exited
                        if job.multi: #this is a multi-node job, we don't have a single state
                            #if any job failed, kill and restart the multi-job
                            if any(j.get('state')=='failed' for j in job.poll().values()): 
                                self.logger.warning('job %s failed',jid)
                                job.kill() #stop the remaining parts so it can be restarted
                                done=False
                        elif job.state != 'done': 
                            self.logger.warning('job %s %s',jid,job.state)
                            done=False
                        if not job.is_alive():
                            if '_' in jid: #set file status if this job is for a file
                                index,filename=jid.split('_',2)
                                try: self.set_file_status(index,filename,job.poll())
                                #file was probably deleted, just log it
                                except Exception as e: self.logger.warning('%s %s',filename,e)
                            del self.__jobs[jid]
                            #if no failure and all jobs have exited
                            if not self.__jobs and done is None: done=True 
                #if we have a max_count set and jobs have run at least max_count times
                if done:
                    job_count+=1 
                    if max_count and job_count>=max_count: break
                
                if globs: #if we are tracking files
                    rescan_count=(rescan_count+1) % self.rescan

                    #rescan files, start jobs on unprocessed files
                    if not rescan_count: #scan files
                        try: 
                            n=self.rescan_path()
                            self.logger.debug('%s: %s files',self.path,n)
                        except Exception as e: self.logger.warning(e)

                        #build lists
                        for glob in globs:
                            self.__files[glob]=[file for file in self.__cache if fnmatch.fnmatch(file.name,glob)]
                            self.logger.debug('%s: %s files',glob,len(self.__files[glob]))

                        #build filesets
                        for glob_index,glob in enumerate(globs): #list index (index of glob->file list)
                            if split and match and glob_index: break #if fileset, only use the first list
                            filesets=[] #sets of files to process
                            for file_index,file in enumerate(self.__files[glob]): #index down file list
                                if max_index and file_index > max_index: break
                                fparts=file.name.split(split) #get filename parts
                                #if match mode
                                fileset=[]
                                fileset_complete=True
                                if split and match:
                                    mpat=split.join(fparts[:match]) #generate match string
                                    for g,files in self.__files.items(): #check across lists
                                        matched=[f for f in files if f.name.startswith(mpat)]
                                        if matched: fileset.extend(matched)
                                        else: 
                                            fileset_complete=False
                                            break
                                    if not fileset_complete and not partial: continue
                                    if skip:
                                        skip_fileset=self.check_file_skip(mpat,split,skip)
                                        if skip_fileset:
                                            self.logger.debug('%s exists, skip fileset %s',skip_fileset, mpat)
                                            continue
                                    self.logger.debug('fileset match %s: %s %s',mpat,fileset,fileset_complete)
                                else: fileset=[file] #not match mode, single file set
                                filesets.append(fileset) #this fileset can be processed 
                            self.logger.debug('%s filesets to check',len(filesets))

                            #check status and start jobs
                            for file_index,fileset in enumerate(filesets):
                                file=fileset[0] #if not matching, fileset will be single file. If matching, first file is key.
                                fparts=file.name.split(split) #get filename parts
                                job_index=min(file_index,len(jobs)-1) #use matching or last job index available
                                #check job 0 to file_index for run_all, just file_ index if not
                                if self.cfg.get('run_all',True): min_index=0
                                else: min_index=job_index
                                for index in range(min_index,job_index+1): 
                                    if not jobs[index]: 
                                        self.logger.debug ("No job at index %s for %s",index,fileset)
                                        continue #no job for this index, skip it.
                                    #check file status
                                    try: 
                                        #job for this file is still running
                                        if '%s_%s'%(index,file.name) in self.__jobs: break 
                                        #is file done (and not updated if updated=1)?
                                        status=self.check_file_status(index,file) 
                                        #if not retrying, failed=done
                                        if not status and not self.cfg.get('retry',True): 
                                            status=self.check_file_status(index,file,'failed')
                                    except Exception as e:
                                        self.logger.warning("%s %s",file.name,e)
                                        status=True #skip this one it smells funny
                                    if not status: #if file is not running and not processed
                                        self.start_job(index,glob_index,file,fparts,fileset=[f.name for f in fileset])
                                        break #if run_all, don't start the next index job until this one is finished
                                    self.logger.debug ("index %s job status %s for %s",index,status,fileset)
                
                else: #just track job
                    for index in range(len(jobs)):
                        if str(index) in self.__jobs: continue #still running
                        self.start_job(index) #start it

                c=0
                while not self.shutdown.is_set():
                    c+=1
                    time.sleep(1)
                    if c > self.refresh: break
        
        #something really bad happened, log it and shut down gracefully
        except Exception as e: self.logger.error(e,exc_info=True)
        
        self.logger.info('stopping')

        #kill all jobs and verify stop before exiting, to ensure client isn't disposed of before sync
        for jid,job in self.__jobs.items(): 
            self.logger.info('killing job %s',jid)
            job.kill(True)

try:
    import xattr

    class WatchXattr(Watch):
        '''replace the set/check file status methods to use filesystem extended attributes'''
        
        def set_file_status(self,index,filename,job=None,status=None):
            '''set the file processing status and last modified time attrs'''
            if job: status=job['state']
            self.logger.debug('set %s %s %s',index,filename,status)
            attr='user.%s_%s'%(self.name,index)
            f=os.path.join(self.path,filename)
            if job: data=json.dumps(job).encode()
            else: data=''.encode()
            xattr.setxattr(f,attr+'.'+job['state'],data)
            if self.cfg.get('updated'): #save mtime
                xattr.setxattr(f,attr+'.mtime',str(os.stat(os.path.join(self.path,filename)).st_mtime).encode())

        def check_file_status(self,index,file,status='done'):
            '''check the file attrs
            should return True if the status=status, False if not'''
            attr='user.%s_%s'%(self.name,index)
            try:
                v=xattr.getxattr(file.path,attr+'.'+status)
                if not self.cfg.get('updated'): return True #is done
                try:
                    mtime=float(xattr.getxattr(file.path,attr+'.mtime'))
                    if mtime != file.stat().st_mtime: return False
                except: pass #no mtime attr, consider done
                return True
            except: return False #attr not set

except: pass #no xattr support available
