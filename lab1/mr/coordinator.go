package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"

type Coordinator struct {
	mutex sync.Mutex
	taskNum int
	taskPhase int
	taskList []Task
}

const (
	MapPhase = 0
	ReducePhase = 1
)

const (
	TaskErr = -1
	TaskReady = 0
	TaskRunning = 1
	TaskDone = 2
)

const timeoutInterval = 3 * time.Second
type Task struct {
	Index     int
	StartTime time.Time
	Status    int
	FileName  string
	NReduce   int
	FileNum   int
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := RegisterService(c)
	if err != nil{
		log.Fatal("Register error:", err)
		return
	}
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("ListenTCP error:", err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatal("Accept error:", err)
			}

			rpc.ServeConn(conn)
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	defer c.mutex.Unlock()
	c.mutex.Lock()
	return c.taskNum == 0 && c.taskPhase == ReducePhase
}

func (c *Coordinator) initMapTask(files []string, nReduce int)  {
	c.taskList = make([]Task, len(files))
	c.taskNum = len(files)
	c.taskPhase = MapPhase
	for i, file := range files{
		c.taskList[i] = Task{
			Index:    i,
			Status:   TaskReady,
			FileName: file,
			NReduce:  nReduce,
			FileNum:  len(files),
		}
	}
}

func (c *Coordinator) initReduceTask(fileNum , nReduce int)  {
	c.taskList = make([]Task, nReduce)
	c.taskNum = nReduce
	c.taskPhase = ReducePhase
	for i := 0; i < nReduce; i++{
		c.taskList[i] = Task{
			Index:    i,
			Status:   TaskReady,
			NReduce:  nReduce,
			FileNum:  fileNum,
		}
	}
}

func (c *Coordinator)RequestTask(args RequestTaskArgs, reply *RequestTaskReply) error  {
	// Use mutexes to prevent a task from being requested twice
	defer c.mutex.Unlock()
	c.mutex.Lock()
	for i := range c.taskList{
		switch c.taskList[i].Status {
		case TaskRunning:
			// task timeout
			// schedule task to another worker
			if time.Now().Sub(c.taskList[i].StartTime) > timeoutInterval{
				fmt.Println("task timeout:", c.taskList[i].FileName)
				c.taskList[i].StartTime = time.Now()
				*reply = RequestTaskReply{
					Task: c.taskList[i],
					Phase: c.taskPhase,
				}
				return nil
			}
		case TaskReady, TaskErr:
			// if task error, redo it
			c.taskList[i].StartTime = time.Now()
			c.taskList[i].Status = TaskRunning
			*reply = RequestTaskReply{
				Task: c.taskList[i],
				Phase: c.taskPhase,
			}
			return nil
		}
	}
	*reply = RequestTaskReply{
		Phase: -1,
	}
	return nil
}

func (c *Coordinator) TaskPhaseTrans(args ReportTaskArgs, reply *ReportTaskReply) error  {
	defer c.mutex.Unlock()
	c.mutex.Lock()
	c.taskList[args.Index].Status = args.Status
	if c.taskList[args.Index].Status == TaskDone{
		if c.taskPhase == MapPhase {
			log.Println("Map task:", c.taskList[args.Index].FileName, "finished")
		}else{
			log.Println("Reduce task:", args.Index, "finished")
		}
		c.taskNum -= 1
	}
	if c.taskNum == 0 && c.taskPhase == MapPhase{
		c.initReduceTask(c.taskList[args.Index].FileNum, c.taskList[args.Index].NReduce)
	}
	return nil
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// initMapTask coordinator
	c.initMapTask(files, nReduce)

	c.server()
	return &c
}
