package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

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
	// Your worker implementation here.
	//register worker and get info
	args := ExampleArgs{}
	mymissioninfo := MissionInfo{}
	ok := call("Coordinator.RegisterWorker", &args, &mymissioninfo)

	for !ok {
		time.Sleep(5 * time.Second)
		fmt.Printf("call failed!\n")
		ok = call("Coordinator.RegisterWorker", &args, &mymissioninfo)
	}

	for {
		job := RequestJob(mymissioninfo.WorkerInfo)
		switch job.JobType {
		case Waiting:
			time.Sleep(2 * time.Second)
		case Exit:
			os.Exit(0)
		case MapJob:
			HandleMap(mymissioninfo, job, mapf)
		case ReduceJob:
			HandleReduce(mymissioninfo, job, reducef)
		}
	}
}

func HandleMap(mission MissionInfo, job JobAssigned, mapf func(string, string) []KeyValue) {
	//call map func and divide intermediate file
	file, err := os.Open(job.Files)
	if err != nil {
		log.Fatalf("cannot open %v", job.Files)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", job.Files)
		return
	}
	file.Close()
	kva := mapf(job.Files, string(content))

	intermedias := make([][]KeyValue, mission.Reducediv)

	if mission.Reducediv == 0 {
		fmt.Println(mission)
		os.Exit(0)
	}

	for _, kvpair := range kva {
		BucketIdx := ihash(kvpair.Key) % mission.Reducediv
		intermedias[BucketIdx] = append(intermedias[BucketIdx], kvpair)
	}
	for i := 0; i < mission.Reducediv; i++ {
		interfile := "mr-" + strconv.Itoa(job.JobIndex) + "-" + strconv.Itoa(i)
		tmpf, err := ioutil.TempFile("./", "tmp-"+interfile)
		defer os.Remove(tmpf.Name())
		if err != nil {
			log.Fatalln("failed to create file! name:", interfile)
			return
		}
		enc := json.NewEncoder(tmpf)
		err = enc.Encode(&(intermedias[i]))
		if err != nil {
			log.Fatalln("failed to encode file! name:", interfile)
			return
		}
		err = os.Rename(tmpf.Name(), interfile)
	}

	//notify coordinator that job is completed
	CallSubmitJob(mission.WorkerInfo, JobInfo{job.JobType, job.JobIndex})
}

func HandleReduce(mission MissionInfo, job JobAssigned, reducef func(string, []string) string) {

	matches, err := filepath.Glob(job.Files)
	files := make([]string, 0)

	if err != nil {
		log.Fatalln(err)
	}
	for _, filename := range matches {
		tmp := strings.Split(filename, "-")
		if len(tmp) != 3 || tmp[0] != "mr" || !isAllDigits(tmp[1]) || !isAllDigits(tmp[2]) {
			break
		}
		files = append(files, filename)
	}

	fmt.Println(files)
	kva := make([]KeyValue, 0)
	for _, filename := range files {
		f, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", job.Files)
			return
		}

		dec := json.NewDecoder(f)
		kvs := make([]KeyValue, 0)
		err = dec.Decode(&kvs)
		if err != nil {
			log.Fatalln("JSON decoding error:", err)
		}
		kva = append(kva, kvs...)
		f.Close()
	}

	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(job.JobIndex)
	tmpf, err := ioutil.TempFile("./", "tmp-"+oname)
	if err != nil {
		log.Fatalln("Error creating temporary file:", err)
	}
	defer os.Remove(tmpf.Name())

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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpf, "%v %v\n", kva[i].Key, output)

		i = j
	}
	err = os.Rename(tmpf.Name(), oname)
	if err != nil {
		log.Fatalln("Error renaming temporary file:", err)
	}
	//notify coordinator that job is completed
	CallSubmitJob(mission.WorkerInfo, JobInfo{job.JobType, job.JobIndex})
}

func isAllDigits(s string) bool {
	for _, ch := range s {
		if !unicode.IsDigit(ch) {
			return false
		}
	}
	return true
}

func RequestJob(worker WorkerInfo) JobAssigned {

	reply := JobAssigned{}

	ok := call("Coordinator.AssignJob", &worker, &reply)
	for !ok {
		time.Sleep(5 * time.Second)
		ok = call("Coordinator.AssignJob", &worker, &reply)
	}
	return reply
}

func CallSubmitJob(worker WorkerInfo, job JobInfo) {
	args := JobSummision{worker, job}
	reply := ExampleReply{}
	ok := call("Coordinator.SubmitJob", &args, &reply)

	if !ok {
		fmt.Println("failed to submit job!")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
