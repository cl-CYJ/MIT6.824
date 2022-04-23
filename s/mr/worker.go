package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


// 实现sort方法，摘自mrsequential.go 
// for sorting by key.
type ByKey []KeyValue
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

	// Your worker implementation here.
	// 启动worker
	for {
		// worker从master获取任务
		task := getTask()

		switch task.TaskType {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
			// fmt.Println("执行wait")
		case Exit:
			return
		}
	}	
}

// worker获取任务
func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Master.AllocateTask", &args, &reply)
	return reply
}


func mapper(task *Task, mapf func (string, string) []KeyValue) {
	content, err := ioutil.ReadFile(task.InputFile)
	if err != nil {
		log.Fatal("Failed to read file :" + task.InputFile, err)
	}
	// 读取文件内容后交给mapf处理
	intermediates := mapf(task.InputFile, string(content))
	// 将中间结果根据key进行hash分成Reduce份写入到本地磁盘
	buffer := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		idx := ihash(intermediate.Key) % task.NReducer
		buffer[idx] = append(buffer[idx], intermediate)
	}
	mapperOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i ++ {
		mapperOutput = append(mapperOutput, writeToLocal(task.TaskNumber, i, &buffer[i]))
	}
	// 发送中间文件的位置
	task.Intermediates = mapperOutput
	TaskCompleted(task)
}

func reducer(task *Task, reducef func (string, []string) string) {
	intermediate := *readFromLocalFile(task.Intermediates)
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file, err")
	}
	// 这部分代码修改自mrsequential.go
	i := 0
	for i < len(intermediate) {
		//将相同的key放在一起分组合并
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//交给reducef，拿到结果
		output := reducef(intermediate[i].Key, values)
		//写到对应的output文件                
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	outName := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tempFile.Name(), outName)
	task.OutputFile = outName
	TaskCompleted(task)
}

func readFromLocalFile(files []string) *[]KeyValue {
	kvs := []KeyValue{}
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if (err != nil) {
			log.Fatal("Failed to open intermeidiate file" + filepath, err)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}
	return &kvs
}

// 通过RPC，将task信息发给master
func TaskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply)
}

func writeToLocal(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-temp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	encoder := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := encoder.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	fileName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), fileName)
	return filepath.Join(dir, fileName)
}


//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		// Master进程结束，worker自动退出
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
