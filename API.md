# meeseeks API

# job spec and attributes

    jobspec: { 
        id: string  #job id, optional, MUST be unique. A UUID will be generated if id is omitted
                    #if an existing job id is given, the job will be modified if possible
        pool: string #pool name, REQUIRED for new jobs
        args: [executable, arg, arg, arg] #The command to run and arguments. If subprocess.Popen likes it, it will work.
        node: string node #optional. Node selection. For new jobs, can be * for all in pool or end with * for wildcard
        stdin: path #path to file to use for the job's stdin
        stdout: path #optional, path to file to use for the job's stdout else stdout_data returns the base64 encoded output
        stderr: path #optional, path to file to use for the job's stderr else stderr_data returns the base64 encoded output
        runtime: int  #optional, maximum runtime of the job. job will be killed and marked as failed if exceeded.
        hold: false|true #optional, if true job will be assigned to a node but not run until set false
        restart: false|true    #if true, job will be restarted on the same node if it exits with success (rc == 0)
        retries: int           #if >0, job will be restarted a max of retries if it exits with failure (rc != 0)
        resubmit:              #if true, when job is finished (done or failed), resubmit it to the submit_node
        config: dict           #pool/task-specific configuration (default sets Popen arguments)
        tags: list             #list of tags, can be matched in query with tag=
        state: new|killed      #set state of job, killed will stop running job, new will restart finished job.
    }
    jobinfo is submitted jobargs plus attributes:
        node: the node the job is assigned to
        submit_node: the node the job was submitted on
        state: the job state (new,running,done,failed,killed)
        active: True if the job is being processed by a node.
                To move a job: kill the job, wait for active=False, then reassign and set state='new'.
        rc: exit code if job done/failed
        error: error details if job did not spawn or exit properly
        stdout_data: base64 encoded stdout after exit if not redirected to a file
        stderr_data: base64 encoded stderr after exit if not redirected to a file
        ts: update timestamp
        seq: sync sequence number. Jobs with the highest seq are most recently updated on this node.
        submit_ts: submit timestamp
        start_ts: job start timestamp
        end_ts: job end timestamp
        start_count: count of time job has started
        fail_count: count of times job has failed

# Client class

the Client object provides a request-oriented API

    from meeseeks import Client

    client=Client(address=None,port=13700,timeout=10,refresh=0,poll=10,expire=60,set_global=False,**cfg)

        address:    the hostname/address of the node to connect to (None=localhost)
        port:       TCP port to connect to (13700)
        timeout:    connect timeout (10 seconds)
        refresh:    interval to sync jobs (0=disabled. Set to >0 to start background thread for cached usage)
        poll:       interval to poll status (10 seconds, must be > refresh)
        expire:     interval to expire jobs from cached state (60 seconds, must be > poll)
        set_global: if True, sets this as the global Client that Job objects will use by default
        cfg:        client connection options, currently supported:
            ssl:    {   #configures SSL for connection, see ssl.SSLContext 
                        capath/cafile:
                        ciphers:
                        options:
                        verify:
                        cert:
                        key:
                        pass:
                    }

    #submit/modify jobs
    client.submit(**jobspec)    # returns { jid:{jobinfo} } for each job started/modified by submit

    #list jobs
    client.ls(<key>=<value>) #returns job ids where <key> in jobinfo equals <value>

    #query job(s)
    client.query(jid)           # returns jid's jobinfo or False if not found
    client.query([jids])        #returns {jid:{jibinfo}} for ids in jids
    client.query(<key>=<value>) #returns all jobs where <key> in jobinfo equals <value>

    #kill job(s)
    client.kill(jid)            #kills/returns jid's jobinfo or False if not found
    client.kill([jids])         #kills/returns {jid:{jibinfo}} for ids in jids
    client.kill(<key>=<value>)  #kills/returns all jobs where <key> in jobinfo equals <value>

    #get cluster info
    client.nodes()              #returns { nodename:{status} for all nodes }
    client.pools()              #returns { poolname:{nodename:slots_available} for online nodes} }

    #send request
    client.request({reqtype:{reqdata}})     #send arbitrary request
        #ex. to push config: 
        Client.request({'config':{..node configuration..}})

    #shutdown
    client.close()              #shutdown background thread and disconnect client

    Using the client cache: 
        Client background thread must be running (init with refresh > 0)
        These methods will immediately update the client's cache, which will be synced to the node every refresh seconds 

    client.submit_job(**jobspec) # returns { jid:{jobinfo} } for each job started/modified by submit

    client.list_jobs(<key>=<value>) #returns job ids where <key> in jobinfo equals <value>

    client.get_job(jid)         #for single job, returns {jobinfo}
    client.get([jids])          #returns {jid:{jibinfo}} for ids in jids
    client.get(<key>=<value>)   #returns all jobs where <key> in jobinfo equals <value>

    client.kill_jobs(jid | [jids] | <key>=<value>)    #kill specified jobs

    client.get_nodes()          #returns { nodename:{status} for all nodes }
    client.get_pools()          #returns { poolname:{nodename:slots_available} for online nodes} }

    client.wait()               #returns after next client sync to node


# Job class

the Job object provides a job-oriented API

    from meeseeks import Job

    job=Job(client=None,notify=None,*args,**jobspec)

        if args are provided, they will set jobspec['args']

        client: the Client object this Job should use.
                If None, the global Client will be used, and initialized if necessary
                The global Client is configured by the environment var MEESEEKS_CONF
                defaults to localhost:13700 with a refresh of 10 seconds

        notify: if notify is set to a callback function, a background thread will be started to track the job
                The  callback will be called with the job object as an argument when job finishes
                For multi-jobs (node=[pattern]*) the callback will be called once for each job exit

    job.<jobinfo key> #jobinfo keys are Job attributes. Any get/set of these attributes will sync changes with the client
    job.jid           #job id or list of ids
    job.info          #cached jobinfo as a dict. Do not modify, will be overwritten
    job.multi         #if true, job is a multi-job set

    job.start() #submits the job, returns the job id from the client. If multi-job spec, returns list of ids

    job.kill()  #kills job

    job.is_alive()  #returns True if job/any jobs are still running

    job.poll()      #returns None if no jobs have exited, or jobinfo if exited
                    #for multi jobs, returns empty dict if none exited, or {jid:info of exited jobs}


    #callback on job exit

    def callback(j):
        exited=j.poll()
        ...

    j=Job(cmd,args..,pool='p1',notify=callback) #create tracked Job object
    j.start() #submit the job and start thread

