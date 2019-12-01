# Submit jobs. It runs them. Then they stop existing.

Knock yourselves out just keep your requests simple.

The meeseeks-box agent runs on each node. 
Head, routing, compute, doesn't matter. It's all the same. 
Just don't connect the nodes in cycles. 

    $ meeseeks-box [config files..]

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
        address: localhost
        port: 13700
        ssl: {SSL config, TODO}
    }

    state: { #configures the state manager
        refresh: 10 #how often in seconds we scan the state
        expire: 300 #how long in seconds a job will persist without being updated
                    #the state completed/failed/killed jobs will be available for this long
    }

    nodes: list of downstream nodes to connect to
        { <nodename>:{
            address: defaults to <nodename>
            port: 13700
            ssl: { ... }
            refresh: 10 (how often in seconds we poll the node and sync state)
            timeout: 10 (timeout in seconds to connect/send/receive data)
        } , ... }

    pools: list of job processing pools
        { <poolname>:{
            slots: null # if set, limit of how many jobs can run simultaneously
            max_runtime: null # if set, limit of how long a job can run for
            refresh: 10 # how often in seconds the queue is scanned for new/finished jobs
            update: 60 # how often in seconds the state of running jobs is updated to prevent expiration
        } , ... }
    }

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
            "id": string , #job id, optional, MUST be unique. A UUID will be generated if id is omitted
            "pool": string , #pool name, REQUIRED.
            "cmd": [executable, arg, arg, arg] , #REQUIRED. the command to run and arguments. If subprocess.Popen likes it, it will work.
        "nodelist": [nodename, ... ], #optional. A list of preferred nodes to use. See Job Routing. 
        
        "stdout": path, #optional, path to file to use for the job's stdout else returns the base64 encoded output
        "stderr": path, #optional, path to file to use for the job's stderr else returns the base64 encoded output
        "stdin": path, #path to file to use for the job's stdin
        "restart_on_exit": false|true,    #if true, job will be restarted on the same node if it exits with success (rc == 0)
        "restart_on_fail": false|true,    #if true, job will be restarted on the same node if it exits with failure (rc != 0)
                                      # if a node fails, the jobs running on it will not be updated and will expire
                                      # these jobs will be failed. If restart_on_fail is set they will be retried once.
                                      # if a nodelist is provided, the job will be reassigned to the first node in the list
                                      # else, the job will wait for the assigned node
      }
        response will be:
        {
            "submit": the job_id, or false if submission failed (no pool, or job_id exists) 
        } 

      "query": job_id
        response will be job dict, or false if job_id does not exist

      "kill": job_id  #kills a job. 
        response will be job dict, or false if job_id does not exist

    } ]

# state sync

Each node periodically pulls node and pool slot availability from connected nodes. This status is not just the status of the peer but also all downstream nodes it has received status from. From this we can determine which nodes are reachable via the peer. Job status for all downstream nldes are synced from the upstream node, then updates are pulled from the downstream node.

# job status

new: submitted job, not yet claimed by a pool. node may be set to the the submit node, pool node, or any intermediate node.

waiting: job is waiting to run in the pool. node is set to the node the job will run on

running: job is running. pid will be set. node is set to the node the job is running on

done: job is finished. stdout and stderr will contain output

failed: job failed. rc will be set, or error will be set. If the job expired, node may be set back to the submit node.
