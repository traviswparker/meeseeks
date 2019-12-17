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
        "node": node #optional. Strict node selection. Job will fail if node unavailable.
        "filter": string #optional. Filter node selection to names containing this pattern.
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
      }
        response will be:
        {
            "submit": the job_id, or false if submission failed 
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

    ./meeseeks-client [options] <command> [args...]

    commands are:
        submit <pool[@node]> <executable> [args....]
            options for submit:
                nodelist= (list of nodes to route job through)
                stdin= stdout= stderr= (redirect job in/out/err to files named)
                restart= (1=restart when done)
                retries= (times to retry if job fails)
                runtime= (max runtime of job)
                id= (job id to submit changes to existing job)
                state= (change existing job state, 'new' will restart finished job)
                hold= (1=queue but do not start job)

        get [jobids | filter] (get all or specified job info as JSON)
            jobids are any number of job id arguments
            filter is any job attributes such as node= pool= state=

        ls [filter] (list job ids)

        set <jobids | filter :> key=value ... (set key=value in jobs matching jobids or filter)
            if a filter is provided, : is used to delimit filter key=value from set key=value

        kill <jobids | filter> (kill job)

        reset <jobids | filter> (restart finished job, reassign new job, or kill/restart running job)

        show [filter] {nodes pools jobs active tree} 
            (prints a foat or tree of cluster status, 
             specify which elements to show, defaults to flat/all)

        nodes (prints full node status JSON)

        pools (prints full pool status JSON)

        config [key=value] [node]
            get/sends config to directly connected or specified node

    generic options are: 
        address= (default localhost)
        port= (defult 13700)
        refresh= (interval to continuously refresh status until no jobs left)
