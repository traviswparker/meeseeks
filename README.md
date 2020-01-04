# Submit jobs. It runs them. Then they stop existing.

Knock yourselves out just keep your requests simple.

The meeseeks-box agent runs on each node. 
Head, routing, compute, doesn't matter. It's all the same. 
Just don't connect the nodes in cycles. 

    $ ./meeseeks-box [config files..]

# configuration 

config files are JSON, some sample configs are included:

    examples/master.cfg
        this is how a master node would be configured
        it connects to the head nodes

    examples/head*.cfg
        this is how a head node would be configured
        it does not have any pools but listens and connects to the compute nodes
    
    examples/node*.cfg
        these are cluster compute nodes
        they each have pools to process jobs
        
in these sample configs, the port number is being changed so all of them can run one one host
generally you don't change the port number, and run one meeseeks-box per host


The config sections, objects, and defaults are as follows:
{

    name: #sets the name of the node, defaults to the hostname if not set
    
    logging: {
        #logging config, use python logging.basicConfig parameters such as level=10 for DEBUG
    }

    defaults: {
         # if a parameter is set here, it will change the default value in all other sections if applicable to that section
    }

    listen: { #configures the listening socket
        address: defaults to localhost
        port: defaults to 13700
        ssl: {SSLContext config}
    }

    state: { #configures the state manager
        expire: 60  # how long in seconds a job will persist without being updated
                    # the state of completed/failed/killed jobs will be available for this long
        expire_active_jobs: true #if set false, active jobs will not be expired
        timeout: 60  #timeout in seconds to receive updated node status before it is marked offline
        file: <filename> #if set, save/reload state from this file)
        checkpoint: <int> #if set, save state to file every <int> seconds)
        history: <filename> #if set, write finished/expired jobs to this file
    }

    nodes: list of downstream nodes to connect to
        { <nodename>:{
            address: defaults to <nodename>
            port: defaults to 13700
            ssl: {SSLContext config}
            refresh: 1 # how often in seconds we sync state
            poll: 10 # how often in seconds we request node/pool status
            timeout: 10 # timeout in seconds to connect/send/receive data
        } , ... }

    pools: list of job processing pools on this node
        { <poolname>:{
            slots: 0 
                # if > 0 sets limit of how many jobs can run simultaneously
                # 0 sets no limit, but nodes with free slots will be preferred
                # -1 drains pool (no new jobs will be assigned)
            max_runtime: null # if set, limit of how long a job can run for
            update: 30 # how often in seconds the state of running jobs is updated to prevent expiration
            plugin: <path.module.Class> to provide this pool instance
        } , ... }

        use_loadavg:  #if set true, load average will be used to select nodes  vs. free pool slots
        wait_in_pool: #if set true, jobs will be assigned to nodes with full pools and run when a slot is free
                      #if false (default) jobs will remain unassigned until a slot is free
    }

config can also be provided on the command line using key.key.key=value

example: 

    $ ./meeseeks-box name=master defaults.refresh=10 state.expire=300 nodes.n11.address=10.0.0.11 nodes.n12.address=10.0.0.12


# JSON request format

 connect with something like
     
     nc localhost 13700
 
 and send JSON.
 newline sends requests for processing.
 double newline disconnects client.

    [ { "status" : {} 
        fetch the cluster status this node knows about
        response will be:
        { 
          "nodes": { nodename:{ ts:..., online:true|false, loadavg:....}, .... },
          "pools": { poolname:{ nodename:slots_available|null, ... }, .... } 
      } 

      "submit" :{ 
        "id": string  #job id, optional, MUST be unique. A UUID will be generated if id is omitted
                        #if an existing job id is given, the job will be modified if possible
        "pool": string #pool name, REQUIRED.
        "args": [executable, arg, arg, arg] #The command to run and arguments. If subprocess.Popen likes it, it will work.
        "node": string #optional. Node selection, can be * for all in pool or end with * for wildcard
        "stdin": path #path to file to use for the job's stdin
        "stdout": path #optional, path to file to use for the job's stdout else stdout_data returns the base64 encoded output
        "stderr": path #optional, path to file to use for the job's stderr else stderr_data returns the base64 encoded output
        "runtime: int  #optional, maximum runtime of the job
        "hold": false|true #optional, if true job will be assigned to a node but not run until set false
        "restart": false|true    #if true, job will be restarted on the same node if it exits with success (rc == 0)
        "retries": int,          #if >0, job will be restarted a max of retrues on the same node if it exits with failure (rc != 0)
                                      # if a node fails, the jobs running on it will not be updated and will expire
                                      # these jobs will be failed. If resries is set they will be retried.
                                      # if a nodelist is provided, the job will be reassigned to the first node in the list
                                      # else, the job will wait for the assigned node
        "state": {new|killed}    #set state of job, killed will stop running job, new will restart finished job
        "tags": [...]            #list of tags, can be matched in query with tag=
      }
        response will be:
        {
            "submit": the job_id:job or false if submission failed 
            job attributes (also includes keys from submit spec):
                node: the node the job is assigned to
                state: the job state (new,running,done,failed,killed)
                active: True if the job is being processed by a node.
                        To move a job: kill the job, wait for active=False, then reassign and set state='new'.
                rc: exit code if job done/failed
                error: error details if job did not spawn or exit
                stdout: base64 encoded stdout after exit if not redirected to a file
                stdout: base64 encoded stderr after exit if not redirected to a file
                ts: update timestamp
                seq: sync sequence number. Jobs with the highest seq are most recently updated on this node.
                submit_ts: submit timestamp
                start_ts: job start timestamp
                end_ts: job end timestamp
                start_count: count of time job has started
                fail_count: count of times job has failed
        } 

      "query": job_id
        response will be job dict, or false if job_id does not exist

      "kill": job_id  #kills a job. 
        response will be job dict, or false if job_id does not exist

      "config": {...} #push a new configuration (if provided) to the node, response is current config
                      #configuration can be pushed to remote nodes via a job in the __config pool
                      #example: {"submit":{"pool":"__config","node":"<node>","args":{<config>}}}
                      #when state is 'done', job args will reflect current config

    } ]

# sync operation

Each node periodically pulls node and pool slot availability from connected nodes. This status is not just the status of the peer but also all downstream nodes it has received status from. From this we can determine which nodes are reachable via the peer. Job status for all downstream nodes are synced from the upstream node, then updates are pulled from the downstream node.

# job state values

new: job has been submitted but not started. May be assigned to a node's pool if wait_in_pool is set.
     if claimed by a pool, node will be set and active=True

running: job is running. pid will be set. node is set to the node the job is running on.

done: job is finished. stdout_data and stderr_data will contain output

failed: job failed. rc will be set, or error will be set. 
        If the job expired and retries are set, node may cleared

killed: job was killed, rc may be set if job was running. 
        if acknowledged and released by node, active=False

# meeseeks-client
    
    meeseeks-client [client-options] <command> [args...]

    or use q-symlinks: q{stat|sub|job|del|mod|conf} [args]

    commands are:
        sub [submit-options] <pool[@node]> <executable> [args....] (submits job, returns job info)
            submit-options:
                pool= sets pool for job to run in
                node= sets node(s) for job to run on. Can be comma-seperated list or end with * for all/wildcard
                stdin= stdout= stderr= (redirect job in/out/err to files named)
                restart= (1=restart when done)
                retries= (times to retry if job fails)
                resubmit= (1=resubmit when job is done or fails)
                runtime= (max runtime of job)
                id= (set new job's id or submit changes to existing job)
                state= (change existing job state, 'new' will restart finished job)
                hold= (1=queue but do not start job)
                tag= list of tags, can be matched in query with tag=

        job|get [jobids|filter] (get all or specified job info as JSON)
            jobids are any number of job id arguments
            filter is any job attributes such as node= pool= state= tag=

        ls [filter] (list job ids)

        del|kill <jobids|filter> (kill job)

        mod|set <jobids|filter : > key=value ... (set key=value in jobs matching jobids or filter, return new job info)
            if a filter is provided, ':' is used to delimit filter key=value from job key=value
            set a job in any finished state (done,failed,killed) to state='new' to restart job
            if node is not specified when restarting job, node will be cleared.

        stat|show [filter] {nodes pools jobs active tree} 
            (prints flat or tree cluster status, 
             specify which elements to show, defaults to flat/all)
             Job Flags: A=active H=hold E=error R=repeating
             
        nodes (prints full node status JSON)

        pools (prints full pool status JSON)

        conf [key=value] [node]
            get/sends config to directly connected or specified node

    client-options can be set by the environment var MEESEEKS_CONF and default to: 
        address= (default localhost)
        port= (defult 13700)
        refresh= (interval to continuously refresh status until no jobs left)

# meeseeks-watch

    meeseeks-watch [key=value]... [config-file]

    watches files, submits jobs on them

    JSON config format, can also be specified on command line as key.subkey..=value|value,value

    {
        "defaults" : { ... defaults for all other sections ... },

        "apply" : <template> apply this template globally to all watches,

        "client" : { ... configuration for connecting to meeseeks ... },
        
        "template" : {
            null|"<name>": { defines a watch template, see spec for watch. 
                            A template with a null name will be auto-applied to all watches }
        },

        "watch" : {
            "<name>": { 
                "template": <name> applies template <name> to this watch config. 
                    Keys defined here override template
                "path" : <path to watch>
                "glob" : <pattern> | [ <pattern>, ... ]
                    Watches the files matching the pattern. 
                    A list of patterns can be specified to watch multiple lists of files
                    Use a glob of "*" for all files
                    If no globs are defined, this watch will simply ensure the jobs defined are always running
                "reverse" : <bool> files are ASCII sorted Z->A, 9->0 to handle datestamps newest to oldest. 
                                    If true, reverse sort the files (oldest to newest)
                "split" : <character> optional character to split filenames on to generate match parts.
                "fileset" : <int> match filename parts across lists to create filesets
                    a fileset is complete when the first <int> parts of a filename in each list matches.
                    files will not be processed until a complete fileset exists. 
                    For example,
                    glob: [*.foo,*.bar,*.baz]
                    split: .
                    match: 2
                    the set will be complete if we have 20200101.00.foo, 20200101.00.bar, 20200101.00.baz
                    default is 0 (no filesets)
                "updated" : <bool> if set, files will be reprocessed if modtime changes. Default false.
                                    If multiple jobs are defined, only the first will be run on file update.
                "retry" : <bool> if true, files with failed jobs will be reprocessed. Default true.
                                    killed jobs will never be retried
                "run_all" : <bool> if true, unprocessed jobs from 0 to the file's index will be sequentially submitted.
                                    if false, only the unprocessed job for the file's index will be submitted.
                                    default true
                "min_age" : <int> if set, file must be at least <int> seconds old to be considered
                "max_age" : <int> if set, file must be newer than <int> seconds to be considered
                "max_index": <int> if set, maximum index down the list we will submit jobs for.
                "refresh" : <int> interval in seconds running jobs are checked and file status updated, default 10
                "rescan"  : <int> interval in seconds files in path are rescanned, default 60

                "jobs" : [
                    { jobspec } | [ {jobspec}, ... ] ,
                        jobspec(s) to submit on first (usually newest) file/fileset in the list(s)
                    ,   
                        {} | [{},...]
                        jobspec(s) to submit on next file/fileset...
                    , ..... ,
                        {} | [{},...]
                        last jobspec(s) will be submitted on all other unprocessed files
                ]
            } 
        }
    }

    if a jobspec is empty or null, do nothing.
    if only one job is defined, this job will be run on all files
    if multiple jobs are defined, files will have the jobs run on them in sequence as more files appear

    for example, when a new file[0] appears:
        jobs[0] will run on new file[0]
        previously processed file[0] will now be file[1], so jobs[1] will run on it
        file[1] will now be file[2] but if jobs[2] does not exist nothing happens.

    if a job is a list of jobspecs:
        the first jobspec will be submitted for files from the first list,    
        the second jobspec for the second list, and so on.
        the highest jobspec will be used for any additional lists.
        lists with an empty/null jobspec will have the files immediately marked as processed

    in jobspecs, the following formats are available for strings:
        %(name)s        name of this watch
        %(filename)s    filename
        %(file)s        full path to file including filename
        %(fileset)s     all filenames in the fileset
        %(fileset<n>)s  if a fileset, will be list<n> filename
        %(<n>)s         part <n> of the filename
        %(index)s       job index
        %(<k>)s         key <k> in the watch config, such as %(path)s

    When a job ends, the job result JSON is written to a hidden file in <path> named:
    ._<name>_<n>_<filename>.<state>
    <name> is the watch name.
    <n> is the index of the job in the jobspec list
    <filename> is the filename
    <state> is the finished job state, typically "done" if successful. 
            If a .done (or .failed if retry=0) file exists the file is considered processed

    If updated=True, last file mtime is tracked in ._<name>_<n>_<filename>.mtime
    hidden ._ files will be deleted when associated files are deleted, unless cleanup=0

    to apply the same watch template to a list of paths, define only the paths in config:
    "watch": {
        "name1":{ "path":"path1" },
        "name2":{ "path":"path2" },
        ...
    }
    and apply template globally with:
        meeseeks-watch apply=<template> templates.cfg paths.cfg
    watch names will be set to <template>-<name>
