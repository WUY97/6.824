package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskStatus int

const (
	NotStarted TaskStatus = iota // 0
	InProgress                   // 1
	Completed                    // 2
	Failed                       // 3
)

const (
	MapTask    = "map"
	ReduceTask = "reduce"
)

const (
	ErrorNoTasksAvailable = "No tasks available"
	ErrInvalidTaskID      = "Invalid task ID"
	ErrInvalidTaskStatus  = "Invalid task status"
	ErrTaskNotInProgress  = "Task not in progress or already completed"
)

type Task struct {
	Status    TaskStatus
	Type      string
	File      string
	Partition int
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	tasks   []Task
	mu      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce

	for i, task := range c.tasks {
		if task.Type == MapTask && task.Status == NotStarted {
			c.tasks[i].Status = InProgress
			reply.TaskID = i
			reply.Task = &c.tasks[i]
			return nil
		}
	}

	if c.allMapTasksDone() {
		for i, task := range c.tasks {
			if task.Type == ReduceTask && task.Status == NotStarted {
				c.tasks[i].Status = InProgress
				reply.TaskID = i
				reply.Task = &c.tasks[i]

				for j := 0; j < len(c.files); j++ {
					filename := fmt.Sprintf("mr-%d-%d", j, task.Partition)
					reply.Files = append(reply.Files, filename)
				}

				return nil
			}
		}
	}

	reply.Task = nil
	reply.Err = ErrorNoTasksAvailable
	return nil
}

func (c *Coordinator) allMapTasksDone() bool {
	return all(c.tasks, func(task Task) bool {
		return task.Type != MapTask || task.Status == Completed
	})
}

func all(tasks []Task, predicate func(Task) bool) bool {
	for _, task := range tasks {
		if !predicate(task) {
			return false
		}
	}
	return true
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskID < 0 || args.TaskID >= len(c.tasks) {
		reply.Success = false
		return errors.New(ErrInvalidTaskID)
	}

	switch args.Status {
	case Completed:
		if c.tasks[args.TaskID].Status == InProgress || c.tasks[args.TaskID].Status == NotStarted {
			c.tasks[args.TaskID].Status = Completed
			reply.Success = true
		} else if c.tasks[args.TaskID].Status == Completed {
			reply.Success = true
		} else {
			reply.Success = false
			return errors.New(ErrTaskNotInProgress)
		}
	case Failed:
		c.tasks[args.TaskID].Status = NotStarted
		reply.Success = true
	default:
		reply.Success = false
		return errors.New(ErrInvalidTaskStatus)
	}

	return nil
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.tasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
		tasks:   make([]Task, len(files)+nReduce),
	}

	// initialize map tasks
	for i, file := range files {
		c.tasks[i] = Task{
			Status: NotStarted,
			Type:   MapTask,
			File:   file,
		}
	}

	// initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		c.tasks[len(files)+i] = Task{
			Status:    NotStarted,
			Type:      ReduceTask,
			Partition: i,
		}
	}

	c.server()
	return &c
}
