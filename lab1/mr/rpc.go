package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"net/rpc"
)

const (
	CoordinatorServiceName = "Coordinator"
)

type RequestTaskArgs struct {
}

type RequestTaskReply struct{
	Task
	Phase int
}

type ReportTaskArgs struct {
	Index  int
	Status int
}

type ReportTaskReply struct {
}

type ServiceInterface = interface {
	RequestTask(args RequestTaskArgs, reply *RequestTaskReply) error
	TaskPhaseTrans(args ReportTaskArgs, reply *ReportTaskReply) error
}


func RegisterService(svc ServiceInterface) error {
	return rpc.RegisterName(CoordinatorServiceName, svc)
}
