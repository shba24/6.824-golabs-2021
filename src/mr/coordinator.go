package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	taskInfo   *TaskInfo
	taskStatus Status
	mu         *sync.Mutex
}

func (t *Task) updateStatus(status Status) {
	t.mu.Lock()
	t.taskStatus = status
	t.mu.Unlock()
}

func (t *Task) casStatus(currentStatus Status, newStatus Status) bool {
	t.mu.Lock()
	ret := false
	if t.taskStatus == currentStatus {
		t.taskStatus = newStatus
		ret = true
	}
	t.mu.Unlock()
	return ret
}

type SubJob struct {
	tasks            map[int]*Task
	queue            chan *TaskInfo
	wg               *sync.WaitGroup
	isSubJobComplete chan bool
}

type Job struct {
	mapSubJob    SubJob
	reduceSubJob SubJob
	isDone       bool
	mu	*sync.Mutex
}

func (j *Job) checkStatus() bool {
	j.mu.Lock()
	ret:= j.isDone
	j.mu.Unlock()
	return ret
}

func (j *Job) setStatus(status bool) {
	j.mu.Lock()
	j.isDone = status
	j.mu.Unlock()
}

/*
StartMapSubJob creates all map tasks using file names and adds all tasks to the task channel
*/
func (j *SubJob) StartMapSubJob(files []string) {
	for idx, file := range files {
		j.wg.Add(1)
		j.tasks[idx] = &Task{
			taskInfo: &TaskInfo{
				Id:        idx,
				TaskType:  MAP,
				InputPath: file,
			},
			taskStatus: PENDING,
			mu: &sync.Mutex{},
		}
		j.queue <- j.tasks[idx].taskInfo
	}
	go func() {
		j.wg.Wait()
		j.isSubJobComplete <- true
	}()
}

/*
StartReduceSubJob creates all reduce tasks using file names and adds all tasks to the task channel
*/
func (j *SubJob) StartReduceSubJob(nReduce int) {
	for idx := 0; idx < nReduce; idx++ {
		j.wg.Add(1)
		j.tasks[idx] = &Task{
			taskInfo: &TaskInfo{
				Id:       idx,
				TaskType: REDUCE,
			},
			taskStatus: PENDING,
			mu: &sync.Mutex{},
		}
	}
	for idx := 0; idx < nReduce; idx++ {
		j.queue <- j.tasks[idx].taskInfo
	}
	go func() {
		j.wg.Wait()
		j.isSubJobComplete <- true
	}()
}

/*
Waits and Starts Reduce Sub Job
*/
func (j *Job) waitAndTriggerReduceSubJob(nReduce int) {
	// Wait for the map sub job to complete
	<-j.mapSubJob.isSubJobComplete
	go j.reduceSubJob.StartReduceSubJob(nReduce)
	go func() {
		<-j.reduceSubJob.isSubJobComplete
		j.setStatus(true)
	}()
}

/*
Starts Map Sub Job
*/
func (j *Job) triggerMapSubJob(files []string) {
	j.mapSubJob.StartMapSubJob(files)
}

type Coordinator struct {
	nReduce int
	nMap    int
	job     *Job
}

/*
[allocateTask] will allocate any remaining pending task to the worker whose
info is provided in the [worker] object.
*/
func (c *Coordinator) AllocateTask(req *AllocateTaskReq, resp *AllocateTaskResp) error {
	mapSubJob := &c.job.mapSubJob
	reduceSubJob := &c.job.reduceSubJob

	select {
	case newT := <-mapSubJob.queue:
		t := mapSubJob.tasks[newT.Id]
		t.updateStatus(RUNNING)
		go func() {
			<-time.After(10 * time.Second)
			if t.casStatus(RUNNING, PENDING) {
				mapSubJob.queue <- t.taskInfo
			}
		}()
		resp.TaskInfo = newT
		resp.NMap = c.nMap
		resp.NReduce = c.nReduce
		log.Printf("Assigning Task %d to Worker %s\n", t.taskInfo.Id, req.WorkerId)
	case newT := <-reduceSubJob.queue:
		t := reduceSubJob.tasks[newT.Id]
		t.updateStatus(RUNNING)
		go func() {
			<-time.After(10 * time.Second)
			if t.casStatus(RUNNING, PENDING) {
				reduceSubJob.queue <- t.taskInfo
			}
		}()
		resp.TaskInfo = newT
		resp.NMap = c.nMap
		resp.NReduce = c.nReduce
		log.Printf("Assigning Task %d to Worker %s\n", t.taskInfo.Id, req.WorkerId)
	default:
		log.Printf("No task available right now")
	}
	return nil
}

/*
[finishTask] will update the coordinator about the finished task given to the worker
with details about the output file.
*/
func (c *Coordinator) FinishTask(req *FinishTaskReq, resp *FinishTaskResp) error {
	mapSubJob := &c.job.mapSubJob
	reduceSubJob := &c.job.reduceSubJob

	switch req.TaskInfo.TaskType {
	case MAP:
		task := mapSubJob.tasks[req.TaskInfo.Id]
		if task.casStatus(RUNNING, SUCCESS) {
			mapSubJob.wg.Done()
			log.Printf("Marking Map task: %d as SUCCESS", task.taskInfo.Id)
		} else {
			log.Printf("Map Task %d is marked as NOT RUNNING\n", task.taskInfo.Id)
		}
	case REDUCE:
		task := reduceSubJob.tasks[req.TaskInfo.Id]
		if task.casStatus(RUNNING, SUCCESS) {
			reduceSubJob.wg.Done()
			log.Printf("Marking Reduce task: %d as SUCCESS", task.taskInfo.Id)
		} else {
			log.Printf("Reduce Task %d is marked as NOT RUNNING\n", task.taskInfo.Id)
		}
	}
	return nil
}

/* [server] start a thread that listens for RPCs from worker.go */
func (c *Coordinator) server() {
	// Prepare UDS socket file
	sockname := coordinatorSock()
	log.Println(sockname)
	if err := os.RemoveAll(sockname); err != nil {
		log.Fatal("UDS file can't be used", err)
	}

	// Register coordinator RPC
	err := rpc.Register(c)
	if err != nil {
		log.Fatal("RPC could not be registered with error ", err)
		return
	}
	rpc.HandleHTTP()

	// Start the UDS server
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			log.Fatal("Failed in serving...", err)
		}
	}()
}

// Done Returns true when the Job is finished otherwise false
func (c *Coordinator) Done() bool {
	return c.job.checkStatus()
}

/*
CreateJob Creates all jobs including creating map tasks and reduce tasks
*/
func CreateJob(files []string, nReduce int) *Job {
	// Initially only map tasks are created, reduce task will be created when all map
	// tasks are finished so that we have immutable input for the reduce tasks
	var mapWg, reduceWg sync.WaitGroup
	job := &Job{
		mapSubJob: SubJob{
			tasks:            make(map[int]*Task),
			queue:            make(chan *TaskInfo, len(files)),
			wg:               &mapWg,
			isSubJobComplete: make(chan bool),
		},
		reduceSubJob: SubJob{
			tasks:            make(map[int]*Task),
			queue:            make(chan *TaskInfo, nReduce),
			wg:               &reduceWg,
			isSubJobComplete: make(chan bool),
		},
		isDone: false,
		mu: &sync.Mutex{},
	}
	job.triggerMapSubJob(files)
	go job.waitAndTriggerReduceSubJob(nReduce)
	return job
}

/*
MakeCoordinator Creates coordinator.
len(files) = Number of Map tasks
nReduce = Number of Reduce tasks
*/
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	job := CreateJob(files, nReduce)
	c := Coordinator{nReduce: nReduce, nMap: len(files), job: job}
	c.server()
	return &c
}
