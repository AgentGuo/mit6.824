package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for{
		t, phase := getOneTask()
		if t == nil{ // get task failed, retry
			time.Sleep(time.Second)
			continue
		}
		if phase == MapPhase {
			fmt.Println("time:", t.StartTime.Format("2006-01-02 15:04:05"),
				",filename:", t.FileName, ",Status:", t.Status, ",Phase", phase)
		}else{
			fmt.Println("time:", t.StartTime.Format("2006-01-02 15:04:05"),
				",nReduce::", t.Index, ",Status:", t.Status, ",Phase", phase)
		}
		doTask(mapf, reducef, t, phase)
	}
}

func getOneTask() (*Task, int) {
	reply := RequestTaskReply{
		Task{},
		0,
	}
	err := call("RequestTask", RequestTaskArgs{}, &reply)
	if err != nil || reply.Phase == -1{
		return nil, 0
	}else{
		return &(reply.Task), reply.Phase
	}
}

func phaseTrans(phase int, taskIndex int) {
	reply := ReportTaskReply{}
	err := call("TaskPhaseTrans", ReportTaskArgs{
		Status: phase,
		Index:  taskIndex,
	}, &reply)
	if err != nil{
		log.Fatal(err)
	}
}

func doTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, t *Task, phase int)  {
	switch phase {
	case MapPhase:
		//time.Sleep(time.Second)
		err := doMap(mapf, t)
		if err != nil{
			phaseTrans(TaskErr, t.Index)
		}else {
			phaseTrans(TaskDone, t.Index)
		}
	case ReducePhase:
		//time.Sleep(time.Second)
		err := doReduce(reducef, t)
		if err != nil{
			phaseTrans(TaskErr, t.Index)
		}else {
			phaseTrans(TaskDone, t.Index)
		}
	}
}

func tempDir(dest string) string {
	tempdir := os.Getenv("TMPDIR")
	if tempdir == "" {
		// Convenient for development: decreases the chance that we
		// cannot move files due to TMPDIR being on a different file
		// system than dest.
		tempdir = filepath.Dir(dest)
	}
	return tempdir
}

func doMap(mapf func(string, string) []KeyValue, t *Task) error {
	intermediates := make([][]KeyValue, t.NReduce)
	file, err := os.Open(t.FileName)
	if err != nil {
		log.Fatal("Map work, open error:", err)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("Map work, read error:", err)
		return err
	}
	file.Close()
	kva := mapf(t.FileName, string(content))
	// divide into nReduce part
	for _, kv := range kva{
		hashIndex := ihash(kv.Key) % t.NReduce
		intermediates[hashIndex] = append(intermediates[hashIndex], kv)
	}
	// save into intermediates file
	for i, intermediate := range intermediates{
		interFileName := getInterFileName(t.Index, i)
		f, err := ioutil.TempFile(tempDir(interFileName), interFileName)
		//f, err := os.Create(interFileName)
		if err != nil{
			log.Fatal("Intermediate file create error:", err)
			return err
		}
		b, err := json.Marshal(intermediate)
		if err != nil{
			log.Fatal("Json marshal error:", err)
			return err
		}
		_, err = f.Write(b)
		if err != nil{
			log.Fatal("Intermediate file write error:", err)
			return err
		}
		err = f.Close()
		if err != nil{
			log.Fatal("file close error:", err)
			return err
		}
		err = os.Rename(f.Name(), interFileName)
		if err != nil{
			log.Fatal("rename file error:", err)
			return err
		}
	}
	return nil
}

func doReduce(reducef func(string, []string) string, t *Task) error {
	intermediate := []KeyValue{}
	for i := 0; i < t.FileNum; i++{
		tmp := []KeyValue{}
		b, err := ioutil.ReadFile(getInterFileName(i, t.Index))
		if err != nil{
			log.Fatal("Reduce work, read err:", err)
		}
		err = json.Unmarshal(b, &tmp)
		if err != nil{
			log.Fatal("Reduce work, unmarshal err:", err)
		}
		intermediate = append(intermediate, tmp...)
	}
	sort.Sort(ByKey(intermediate))
	outputFile := "mr-out-"+strconv.Itoa(t.Index)
	ofile, _ := ioutil.TempFile(tempDir(outputFile), outputFile)
	for i := 0; i < len(intermediate);{
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), outputFile)
	for i := 0; i < t.FileNum; i++ {
		os.Remove(getInterFileName(i, t.Index))
	}
	return nil
}

func getInterFileName(taskIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", taskIndex, reduceIndex)
}
// send an RPC request to the coordinator, wait for the response.
func call(rpcname string, args interface{}, reply interface{}) error {
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("Dialing error:", err)
		return err
	}
	err = client.Call(CoordinatorServiceName+"."+rpcname, args, reply)
	if err != nil {
		log.Fatal("Call error:", err)
		return err
	}
	err = client.Close()
	if err != nil {
		log.Fatal("Close error:", err)
		return err
	}
	return nil
}
