package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

type interKV struct {
	Key string			`json:"key"`
	Values []string 	`json:"values"`
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func createId(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

type WorkerInfo struct {
	id      string
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *WorkerInfo) tempOutFile(taskId int, partition int) string {
	return fmt.Sprintf("mr-tmp-%+v-%+v", taskId, partition)
}

func (w *WorkerInfo) outputFileName(taskId int, partition int) string {
	return fmt.Sprintf("mr-%+v-%+v", taskId, partition)
}

func (w *WorkerInfo) tempFinalFile(partition int) string {
	return fmt.Sprintf("mr-tmp-reduce-%+v", partition)
}

func (w *WorkerInfo) finalFileName(partition int) string {
	return fmt.Sprintf("mr-out-%+v", partition)
}

func (w *WorkerInfo) MapHandler(taskInfo *TaskInfo, nReduce int)  {
	// Read Input file and call [mapf] function
	file, err := os.Open(taskInfo.InputPath)
	if err != nil {
		log.Fatalf("cannot open %v, err: %v", taskInfo.InputPath, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v, err: %s\n", taskInfo.InputPath, err)
	}
	err = file.Close()
	if err != nil {
		log.Println("Failed to close the file handle.")
	}
	kva:= w.mapf(taskInfo.InputPath, string(content))

	// Open the output files
	mapOutput:= make([]*os.File, nReduce)
	for pid:=0;pid<nReduce;pid++ {
		filename:= w.tempOutFile(taskInfo.Id, pid)
		ofile, err:= os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Failed to open the output file: %s, err:%s\n", filename, err)
		}
		mapOutput[pid] = ofile
	}

	// Defer the function to close the output files
	defer func() {
		for idx, ofile:= range mapOutput {
			_ = ofile.Close()
			err := os.Rename(ofile.Name(), w.outputFileName(taskInfo.Id, idx))
			if err != nil {
				log.Fatalf("Failed to rename the output file")
			}
		}
		log.Printf("Finished completing task: %d, filename: %s\n", taskInfo.Id, taskInfo.InputPath)
		CallfinishTask(taskInfo)
	}()

	for _, kv:= range kva {
		text, err:= json.Marshal(kv)
		if err != nil {
			log.Fatalf("Failed to marshal content")
		}

		partition:= ihash(kv.Key) % nReduce
		_, err = fmt.Fprintln(mapOutput[partition], string(text))
		if err != nil {
			log.Fatalf("Failed to write to the output file")
		}
	}
}

func (w *WorkerInfo) ReduceHandler(taskInfo *TaskInfo, nMap int) {
	var kva []KeyValue
	// Open the input files
	for pid:=0;pid<nMap;pid++ {
		ofile, err:= os.Open(w.outputFileName(pid, taskInfo.Id))
		if err != nil {
			log.Fatalf("Failed to open the input file: %s, err: %s\n", ofile.Name(), err)
		}

		scanner:= bufio.NewScanner(ofile)
		for scanner.Scan() {
			var intermediate KeyValue
			if e := json.Unmarshal(scanner.Bytes(), &intermediate); e != nil {
				log.Fatalf("Failed to unmarshal intermediate file")
			}
			kva = append(kva, intermediate)
		}
		err = ofile.Close()
		if err != nil {
			log.Fatalf("Failed to close the input file")
		}
	}

	sort.Sort(ByKey(kva))

	ofile, err:= os.OpenFile(w.tempFinalFile(taskInfo.Id), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open the output file: %s, err: %s\n", ofile.Name(), err)
	}
	defer func() {
		_ = ofile.Close()
		err := os.Rename(ofile.Name(), w.finalFileName(taskInfo.Id))
		if err != nil {
			log.Fatalf("Failed to rename the output file")
		}
		log.Printf("Finished completing task: %s\n", taskInfo.Id)
		CallfinishTask(taskInfo)
	}()

	for i:=0; i < len(kva); {
		j:=i+1
		for j<len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		key := kva[i].Key
		var values []string
		for k:=i;k<j;k++ {
			values = append(values, kva[k].Value)
		}
		newValue:= w.reducef(key, values)
		_, err := fmt.Fprintf(ofile, "%v %v\n", key, newValue)
		if err != nil {
			log.Fatalf("Failed to write to the output file")
		}
		i = j
	}
}

func (w *WorkerInfo) ProcessTask(taskInfo *TaskInfo, nReduce int, nMap int) {
	if taskInfo.TaskType == MAP {
		w.MapHandler(taskInfo, nReduce)
	} else {
		w.ReduceHandler(taskInfo, nMap)
	}
}

func (w *WorkerInfo) PollCoordinator() {
	for {
		<-time.After(100 * time.Millisecond)
		taskInfo, nReduce, nMap := CallAllocateTask(w.id)
		if taskInfo != nil {
			w.ProcessTask(taskInfo, nReduce, nMap)
		} else {
			break
		}
	}
}

/*
ihash(key) deterministically maps the string to integer.
use ihash(key) % NReduce to choose the reduce
task number for each KeyValue emitted by Map.
*/
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker := WorkerInfo{id: createId(4), mapf: mapf, reducef: reducef}
	worker.PollCoordinator()
}

/*
CallAllocateTask
Calls RPC function Coordinator.allocateTask
*/
func CallAllocateTask(workerId string) (*TaskInfo, int, int) {
	req := AllocateTaskReq{WorkerId: workerId}
	resp := AllocateTaskResp{}

	// send the RPC request, wait for the reply.
	if call("Coordinator.AllocateTask", &req, &resp) != true {
		log.Fatalf("Coordinator.allocateTask RPC call failed")
	}

	// Print the received task
	fmt.Printf("Recived Task %v , nReduce %d\n", resp.TaskInfo, resp.NReduce)

	return resp.TaskInfo, resp.NReduce, resp.NMap
}

/*
CallfinishTask
Calls RPC function Coordinator.finishTask
*/
func CallfinishTask(taskInfo *TaskInfo) {
	req := FinishTaskReq{TaskInfo: taskInfo}
	resp := FinishTaskResp{}

	// send the RPC request, wait for the reply.
	if call("Coordinator.FinishTask", &req, &resp) != true {
		log.Fatalf("Coordinator.finishTask RPC call failed")
	}
	return
}

/*
[call]
send an RPC request to the coordinator, wait for the response.
usually returns true.
returns false if something goes wrong.
*/
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func(c *rpc.Client) {
		err := c.Close()
		if err != nil {
			log.Fatal("Failed to close the client connection")
		}
	}(c)

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
