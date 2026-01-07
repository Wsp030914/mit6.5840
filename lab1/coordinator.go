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

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Finished
)
const taskTimeout = 10 * time.Second

type Coordinator struct {
	mu sync.Mutex

	nReduce         int
	reduceState     []TaskState
	reduceStartTime []time.Time
	reduceAttempt   []int64
	reduceDone      int

	maptask      int
	mapState     []TaskState
	mapStartTime []time.Time
	mapAttempt   []int64
	mapDone      int

	phase     string
	filenames []string
	donechan  chan int
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case "DonePhase":
		reply.TaskType = TaskExit
		return nil
	case "MapPhase":
		c.getmapTask(reply)
	case "ReducePhase":
		c.getreduceTask(reply)
	}
	return nil

}

func (c *Coordinator) getreduceTask(reply *RequestTaskReply) {
	if c.reduceDone == c.nReduce {
		c.phase = "DonePhase"
		reply.TaskType = TaskExit
		return
	}
	taskid := -1
	for i := range c.reduceState {
		if c.reduceState[i] == Idle {
			taskid = i
			break
		}
	}
	if taskid == -1 {
		reply.TaskType = TaskWait
	} else {
		c.reduceState[taskid] = InProgress
		c.reduceStartTime[taskid] = time.Now()
		attempt := time.Now().UnixNano()
		c.reduceAttempt[taskid] = attempt
		reply.Attempt = attempt
		reply.TaskType = TaskReduce
		reply.TaskID = taskid
		reply.NReduce = c.nReduce
		reply.NMap = c.maptask
	}

}
func (c *Coordinator) getmapTask(reply *RequestTaskReply) {
	if c.mapDone == c.maptask {
		c.phase = "ReducePhase"
		c.getreduceTask(reply)
		return
	}
	taskid := -1
	for i := range c.mapState {
		if c.mapState[i] == Idle {
			taskid = i
			break
		}
	}
	if taskid == -1 {
		reply.TaskType = TaskWait
	} else {
		c.mapState[taskid] = InProgress
		c.mapStartTime[taskid] = time.Now()
		attempt := time.Now().UnixNano()
		c.mapAttempt[taskid] = attempt
		reply.Attempt = attempt
		reply.TaskType = TaskMap
		reply.TaskID = taskid
		reply.FileName = c.filenames[taskid]
		reply.NReduce = c.nReduce
		reply.NMap = c.maptask
	}
}

func (c *Coordinator) RequestTaskDone(args *RequestTaskDoneArgs, reply *RequestTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case TaskMap:
		if args.Attempt != c.mapAttempt[args.TaskID] {
			return nil
		}
		c.AddMapDone(args.TaskID)
	case TaskReduce:
		if args.Attempt != c.reduceAttempt[args.TaskID] {
			return nil
		}
		c.AddReduceDone(args.TaskID)
	}
	return nil
}

func (c *Coordinator) AddReduceDone(TaskID int) {
	c.reduceState[TaskID] = Finished
	c.reduceDone++
}
func (c *Coordinator) AddMapDone(TaskID int) {
	c.mapState[TaskID] = Finished
	c.mapDone++
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
    done := (c.phase == "DonePhase")
    c.mu.Unlock()
    if done {
        select {
        case c.donechan <- 1:
        default:
        }
    }
	return done
}

func (c *Coordinator) checkstate() {
	for {
		select {
		case <-c.donechan:
			return
		default:
			c.mu.Lock()
			c.TraverseState()
			c.mu.Unlock()
		}
		time.Sleep(1 * time.Second)
	}
}

func (c *Coordinator) TraverseState() {
	switch c.phase {
	case "MapPhase":
		for i := range c.mapState {
			if c.mapState[i] == InProgress && time.Since(c.mapStartTime[i]) > taskTimeout {
				c.mapState[i] = Idle
			}
		}
	case "ReducePhase":
		for i := range c.reduceState {
			if c.reduceState[i] == InProgress && time.Since(c.reduceStartTime[i]) > taskTimeout {
				c.reduceState[i] = Idle
			}
		}
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:         nReduce,
		reduceState:     make([]TaskState, nReduce),
		reduceStartTime: make([]time.Time, nReduce),
		reduceAttempt:   make([]int64, nReduce),
		phase:           "MapPhase",
		filenames:       files,
		maptask:         len(files),
		mapState:        make([]TaskState, len(files)),
		mapStartTime:    make([]time.Time, len(files)),
		mapAttempt:      make([]int64, len(files)),
		donechan:        make(chan int, 1),
	}
	go c.checkstate()

	c.server()
	return &c
}
