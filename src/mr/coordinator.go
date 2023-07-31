package mr

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "sync"

type Coordinator struct {
	// Your definitions here.
	completed bool
	Phase
	MapJobStatus    []int
	ReduceJobStatus []int
	Workers         []Workerstatu
	M               int
	R               int
	MCompleted      int
	RCompleted      int
	nWorkers        int
	mu              sync.Mutex
	cond            *sync.Cond
	jobList         *LinkedList
	mapfiles        []string
}

type Workerstatu struct {
	isworking bool
	JobType
	JobIndex     int
	JobStartTime time.Time
}

type JobNode struct {
	JobType
	JobIndex int
	File     string
}

type Phase = int
type Jobstatus = int

const (
	MapPhase Phase = iota
	ReducePhase
)

const (
	idle Jobstatus = iota
	working
	complete
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) SubmitJob(args *JobSummision, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error = nil
	worker := args.WorkerInfo.Name
	jobtype := args.JobType
	jobidx := args.JobIndex
	c.Workers[worker] = Workerstatu{false, idle, -1, time.Time{}}
	switch jobtype {
	case MapJob:
		if c.MapJobStatus[jobidx] == complete {
			err = errors.New("job already done")
			return err
		}
		c.MapJobStatus[jobidx] = complete
		c.MCompleted++
		if c.M == c.MCompleted {
			c.Phase = ReducePhase
			c.jobList.Clear()
			for i := 0; i < c.R; i++ {
				c.jobList.InsertAtHead(JobNode{ReduceJob, i, "mr-*-" + strconv.Itoa(i)})
			}
		}
	case ReduceJob:
		if c.ReduceJobStatus[jobidx] == complete {
			err = errors.New("job already done")
			return err
		}
		fmt.Printf("RC:%d,status before:%v \n", c.RCompleted, c.ReduceJobStatus)
		c.ReduceJobStatus[jobidx] = complete
		c.RCompleted++
		fmt.Printf("RC:%d,status before:%v \n", c.RCompleted, c.ReduceJobStatus)
		if c.R == c.RCompleted {
			c.completed = true
			c.jobList.Clear()
		}
	}
	return err
}

func (c *Coordinator) RegisterWorker(args *ExampleArgs, reply *MissionInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	worker := Workerstatu{false, Waiting, -1, time.Time{}}
	c.Workers = append(c.Workers, worker)
	reply.WorkerInfo.Name = c.nWorkers
	reply.Reducediv = c.R
	c.nWorkers++
	return nil
}

func (c *Coordinator) AssignJob(args *WorkerInfo, reply *JobAssigned) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.completed {
		reply.JobType = Exit
		c.Workers[args.Name] = Workerstatu{false, Exit, -1, time.Time{}}
		return nil
	}

	//if this worker already request a job before
	//but didn't complete for some reason
	//resume the job
	//what if worker proc restart??

	node := c.jobList.DeleteAtHead()
	var err error = nil
	if node == nil {
		reply.JobType = Waiting
		err = errors.New("no job in job list")
	} else {
		jobdata, _ := node.(JobNode)
		reply.JobType = jobdata.JobType
		reply.Files = jobdata.File
		reply.JobIndex = jobdata.JobIndex
		switch jobdata.JobType {
		case MapJob:
			c.MapJobStatus[jobdata.JobIndex] = working
		case ReduceJob:
			c.ReduceJobStatus[jobdata.JobIndex] = working
		}
		statue := Workerstatu{true, jobdata.JobType, jobdata.JobIndex, time.Now()}
		c.Workers[args.Name] = statue
	}
	return err
}

func (c *Coordinator) checkworkers() {
	for {
		c.mu.Lock()
		fmt.Printf("map jobs:%v \n", c.MapJobStatus)
		fmt.Printf("reduce jobs:%v \n", c.ReduceJobStatus)
		fmt.Printf("workers:%v \n", c.Workers)
		fmt.Printf("MComplete:%v \n", c.MCompleted)
		fmt.Printf("RComplete:%v \n", c.RCompleted)
		if c.completed {
			return
		}
		for name, worker := range c.Workers {
			if worker.isworking && time.Since(worker.JobStartTime) > 10*time.Second {
				c.Workers[name].isworking = false
				var jobfile string
				if worker.JobType == MapJob {
					jobfile = c.mapfiles[worker.JobIndex]
					c.MapJobStatus[worker.JobIndex] = idle
				} else if worker.JobType == ReduceJob {
					jobfile = "mr-*-" + strconv.Itoa(worker.JobIndex)
					c.ReduceJobStatus[worker.JobIndex] = idle
				}
				c.jobList.InsertAtHead(JobNode{worker.JobType, worker.JobIndex, jobfile})
			}
		}
		c.mu.Unlock()
		time.Sleep(10 * time.Second)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	//ret := c.completed

	if c.completed {
		for {
			var flag = false
			for _, worker := range c.Workers {
				if worker.JobType != Exit {
					flag = true
					break
				}
			}
			if !flag {
				break
			}
			time.Sleep(10 * time.Second)
		}
	}
	// Your code here.

	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	matches, err := filepath.Glob(files[0])
	if err != nil {
		fmt.Println("Error:", err)
	}
	c := Coordinator{false, MapPhase, make([]int, len(matches)),
		make([]int, nReduce), make([]Workerstatu, 0),
		len(matches), nReduce, 0, 0, 0, sync.Mutex{},
		&sync.Cond{}, NewLinkedList(), matches}
	// Your code here.
	//add map jobs
	for i := 0; i < len(matches); i++ {
		c.jobList.InsertAtHead(JobNode{MapJob, i, matches[i]})
	}
	c.cond = sync.NewCond(&c.mu)
	c.server()
	go c.checkworkers()
	return &c
}
