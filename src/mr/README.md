Requirements:
1. Implement Worker process in worker.go
    * Takes Map and Reduce function as an input and calls 
      Map or Reduce depending on the task type.
    * Handles sequential read and writes.
    * Handles atomic read and atomic writes.
    * Talks to the coordinator via RPC and poll.
    * Asks Coordinator process for the task consisting of
      task type, task input file which worker will read from
      the local file system and task output file which worker
      will write to.
    * Should divide the intermediate keys into buckets for `nReduce`
      reducer tasks.
    * Terminate at the end of the task after telling `coordinator.go`
      of the output of the task.
    * Use `mr-X-Y` as the intermediate file name where `X` represents
      the Map task id and `Y` represents Reduce task id.
    * Use json marshall and unmarshall for intermediate file representation.
    * Use `ihash()` function to partition keys to respective partition.
    * Reducer can't start until the last map function is finished.

2. Implement Coordinator process in coordinator.go
    * Assigned a Job and splits it into tasks
    * Maintains the status of tasks
    * Hands out the tasks to available worker process
    * Cops with failed worker process
    * Check the status of tasks after each 10 seconds and if
      not completed, re-trigger the task on another worker process.
    * Each input file corresponds to one partition and should
      be assigned to only one worker Map task.
    * Implement `Done` function which will tell `mrcoordinator.go`
      that MapReduce task is finished.
      
3. RPC required
    * Worker
        * ping() : Void
      
    * Coordinator
        * allocateTask(string worker_id) : Task Object
        * finishTask(string TaskId,string outputFile): Void
    
How to run:
1. Use `-race` flag while compiling the go code to detect the race conditions.
2. Do not change `main/mrcoordinator.go` and `main/mrworker.go`.
3. Implementation should be in `mr/coordinator.go`, `mr/worker.go`
   and `mr/rpc.go`
   
4. Run coordinator as `go run -race mrcoordinator.go pg-*.txt` and
   worker as `go run -race mrworker.go wc.so`
   
5. How to test: run `cd ~/6.824/src/main && bash test-mr.sh`
6. Modify `mr/worker.go`, `mr/coordinator.go` and `mr/rpc.go`.