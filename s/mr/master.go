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

type TaskStatus int
type State int

var mutex sync.Mutex

const (
	Map State = iota
	Reduce
	Exit
	Wait // 规则上指出如果有些worker需要等待(例如Map任务都分发出去，又有worker来申请，他需要等待所有Map完成后才能申请Reduce任务)
)

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

/*
	对于一个Map任务和Reduce任务，二者结构的区别仅在于任务的类型不同，其他属性都是通用且必需的
*/
type Task struct {
	TaskSta TaskStatus  //  task的状态
	TaskType State		// 任务类型
	StartTime time.Time // 任务开始的时间，用于检测worker是否正常工作，Hint中写到，当worker工作时间超过十秒，就应该假设他已经死亡(即使有可能并没有)
	TaskNumber int 		// 任务编号
	NReducer int		
	InputFile string	// 输入文件
	OutputFile string	// 输出文件
	Intermediates []string	// 产生的中间文件的信息
}

type Master struct {
	// Your definitions here.
	TaskQueue chan *Task // 任务队列，go中chan是线程安全的
	TaskInfo	map[int]*Task // 所有task的信息
	MasterPhase	State	// Master执行到了哪个阶段
	NReduce		int		// 传入的NReduce
	InputFiles	[]string // 输入的文件名
	Intermediates [][]string // Map输出的中间文件的信息
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	mutex.Lock()
	defer mutex.Unlock()
	// Your code here.
	ret := (m.MasterPhase == Exit)
	return ret
}

func max(a , b int) int {
	if a > b {
		return a
	}
	return b
}

func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	// 更新task状态
	mutex.Lock()
	defer mutex.Unlock()
	if task.TaskType != m.MasterPhase || m.TaskInfo[task.TaskNumber].TaskSta == Completed {
		// 此lab并不涉及多副本保存，对于重复的结果要丢弃
		return nil
	}
	m.TaskInfo[task.TaskNumber].TaskSta = Completed
	go m.checkChangePhase(task)
	return nil
}

func (m *Master) checkChangePhase(task *Task) {
	mutex.Lock()
	defer mutex.Unlock()
	switch task.TaskType {
	case Map:
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		if m.allTaskDone() {
			// 所有map任务完成后，进入reduce阶段
			m.createReduceTasks()
			m.MasterPhase = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			// 所有reduce任务完成后，进入Exit状态
			m.MasterPhase = Exit
		}
	}
}

func (m *Master) allTaskDone() bool {
	for _, task := range m.TaskInfo {
		if task.TaskSta != Completed {
			return false
		}
	}
	return true
}

func (m *Master) AllocateTask(args *ExampleArgs, reply *Task) error {
	if len(m.TaskQueue) > 0 {
		*reply = *<-m.TaskQueue

		m.TaskInfo[reply.TaskNumber].TaskSta = InProgress
		m.TaskInfo[reply.TaskNumber].StartTime = time.Now()
	} else if m.MasterPhase == Exit {
		*reply = Task{TaskType: Exit}
	} else {
		// 没有task，让worker等待此阶段执行完
		*reply = Task{TaskType: Wait}
	}
	return nil
}

func (m *Master) createMapTasks() {
	for idx, file := range m.InputFiles {
		m.TaskInfo[idx] = &Task {
			InputFile: file,
			TaskType: Map,
			NReducer: m.NReduce,
			TaskNumber: idx,
			TaskSta: Idle,
		}
		m.TaskQueue <- m.TaskInfo[idx]
	}
}

func (m *Master) createReduceTasks() {
	m.TaskInfo = make(map[int]*Task)
	for idx, files := range m.Intermediates {
		m.TaskInfo[idx] = &Task{
			TaskSta:  Idle,
			TaskNumber: idx,
			Intermediates: files,
			NReducer: m.NReduce,
			TaskType: Reduce,
		}
		m.TaskQueue <- m.TaskInfo[idx]
	}
}

// 监视是否有worker宕掉
func (m *Master) inspect() {
	for {
		time.Sleep(5 * time.Second)
		mutex.Lock()
		if (m.MasterPhase == Exit) {
			mutex.Unlock()
			return
		}
		for _, task := range m.TaskInfo {
			if (task.TaskSta == InProgress && (time.Now().Sub(task.StartTime)) > 10 * time.Second) {
				task.TaskSta = Idle
				m.TaskQueue <- task
			}
		}
		mutex.Unlock()
	}
}


//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		TaskQueue: make(chan *Task, max(nReduce, len(files))),
		TaskInfo: make(map[int]*Task),
		MasterPhase: Map,
		NReduce: nReduce,
		InputFiles: files,
		Intermediates: make([][]string, nReduce),
	}

	// Your code here.
	m.createMapTasks()

	m.server()

	go m.inspect()
	return &m
}
