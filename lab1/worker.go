package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply, err := CallRequestTask()
		if err != nil {
			log.Printf("RequestTask RPC failed: %v; retrying...", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		switch reply.TaskType {
		case TaskMap:
			err := Maptask(mapf, reply)
			if err == nil {
				CallTaskDone(reply.TaskType, reply.TaskID, reply.Attempt)
			}else{
				time.Sleep(300 * time.Millisecond)
			}
		case TaskReduce:
			err := ReduceTask(reducef, reply)
			if err == nil {
				CallTaskDone(reply.TaskType, reply.TaskID, reply.Attempt)
			}else{
				time.Sleep(300 * time.Millisecond)
			}
		case TaskWait:
			time.Sleep(300 * time.Millisecond)
		case TaskExit:
			return
		}
	}
}

func ReduceTask(reducef func(string, []string) string, reply *RequestTaskReply) error {
	kva := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		filePath := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskID)
		file, err := os.Open(filePath)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))
	finalPath := "mr-out-" + strconv.Itoa(reply.TaskID)
	f, tmp, err := openTempForAtomic(finalPath)
	if err != nil {
		return err
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		_, err := fmt.Fprintf(f, "%v %v\n", kva[i].Key, output)
		if err != nil {
			abortTmp(f, tmp)
			return err
		}
		i = j
	}
	closeOnly(f)
	err = atomicRename(tmp, finalPath)
	if err != nil {
		removeTmp(tmp)
		return err
	}
	return nil
}

func Maptask(mapf func(string, string) []KeyValue, reply *RequestTaskReply) error {
	file, err := os.Open(reply.FileName)
	if err != nil {
		return err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))
	files := make([]*os.File, reply.NReduce)
	tmps := make([]string, reply.NReduce)
	encs := make([]*json.Encoder, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		finalPath := "mr-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(i)
		f, tmp, err := openTempForAtomic(finalPath)
		if err != nil {
			for j := 0; j < i; j++ {
				abortTmp(files[j], tmps[j])
				files[j] = nil
				tmps[j] = ""
				encs[j] = nil
			}
			return err
		}
		files[i] = f
		tmps[i] = tmp
		encs[i] = json.NewEncoder(f)
	}

	for i := 0; i < len(kva); i++ {
		ReduceID := ihash(kva[i].Key) % reply.NReduce
		err := encs[ReduceID].Encode(&kva[i])
		if err != nil {
			for j := 0; j < reply.NReduce; j++ {
				abortTmp(files[j], tmps[j])
			}
			return err
		}

	}

	for i := 0; i < reply.NReduce; i++ {
		closeOnly(files[i])
		finalPath := "mr-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(i)
		err := atomicRename(tmps[i], finalPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func CallRequestTask() (*RequestTaskReply, error) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		err := errors.New("worker 启动失败")
		return &reply, err
	}
	return &reply, nil
}

func CallTaskDone(TaskType TaskType, TaskID int, Attempt int64) error {
	args := RequestTaskDoneArgs{
		TaskType: TaskType,
		TaskID:   TaskID,
		Attempt:  Attempt,
	}
	reply := RequestTaskDoneReply{}
	ok := call("Coordinator.RequestTaskDone", &args, &reply)
	if !ok {
		err := errors.New("WorkerDone 失败")
		return err
	}
	return nil
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func openTempForAtomic(finalPath string) (f *os.File, tmpPath string, err error) {
	dir := filepath.Dir(finalPath)
	base := filepath.Base(finalPath)

	pattern := "." + base + ".tmp-*"

	f, err = os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, "", err
	}

	tmpPath = f.Name()
	return f, tmpPath, nil
}

func removeTmp(tmp string) {
	if tmp != "" {
		_ = os.Remove(tmp)
	}
}

func closeOnly(f *os.File) {
	if f != nil {
		_ = f.Close()
	}
}

func abortTmp(f *os.File, tmp string) {
	closeOnly(f)
	removeTmp(tmp)
}

func atomicRename(tmpPath string, finalPath string) error {
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}
