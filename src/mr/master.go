package mr

import "log"
import "net"
import "os"
import "io/ioutil"
import "net/rpc"
import "net/http"
import "container/heap"
import "time"
import "sync"


type Item struct {
	task *Task
	priority int64 
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item
var priorityQueueMutex sync.Mutex

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	priorityQueueMutex.Lock()
	defer priorityQueueMutex.Unlock()
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	priorityQueueMutex.Lock()
	defer priorityQueueMutex.Unlock()
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	priorityQueueMutex.Lock()
	defer priorityQueueMutex.Unlock()
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, task *Task, priority int64) {
	priorityQueueMutex.Lock()
	defer priorityQueueMutex.Unlock()
	item.task = task
	item.priority = priority
	heap.Fix(pq, item.index)
}

type Task struct {
	Id int
	Task_type TaskType
	Filename string
	Content [] byte // for map
	Hash_key int// for reduce
}

type SafeTaskQueue struct {
	lock sync.Mutex
	tasks [] *Task
}

type Master struct {
	processing_queue PriorityQueue
	wait_tasks SafeTaskQueue 
	processing_tasks SafeTaskQueue 
	done_tasks SafeTaskQueue 
	finished bool
	reissue_delta int64
	nreduce int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) FetchTasks(args *Args, reply *Reply) error {
	m.wait_tasks.lock.Lock()
	defer m.wait_tasks.lock.Unlock()
	if len(m.wait_tasks.tasks) > 0 {
		task := m.wait_tasks.tasks[len(m.wait_tasks.tasks)-1]
		// log.Printf("Fetch task, before: %v\n", len(m.wait_tasks.tasks))
		reply.Task = task
		reply.Nreduce = m.nreduce

		m.wait_tasks.tasks = m.wait_tasks.tasks[:len(m.wait_tasks.tasks) - 1]
		// log.Printf("after: %v\n", len(m.wait_tasks.tasks))

		m.processing_tasks.lock.Lock()
		m.processing_tasks.tasks = append(m.processing_tasks.tasks, task)
		m.processing_tasks.lock.Unlock()

		// push timesteamp of now
		item := Item{task, time.Now().Unix(), 0}
		m.processing_queue.Push(&item)
	} else {
		task := Task {Task_type: WaitTask}
		reply.Task = &task
	}
	return nil
}


func (m *Master) ReplyTasks(args *AnswerArgs, reply *AnswerReply) error {
	m.processing_tasks.lock.Lock()
	defer m.processing_tasks.lock.Unlock()
	for i, task := range m.processing_tasks.tasks {
		if task.Id == args.Task_id {
			m.done_tasks.lock.Lock()
			defer m.done_tasks.lock.Unlock()
			m.done_tasks.tasks = append(m.done_tasks.tasks, task)
			m.processing_tasks.tasks = append(m.processing_tasks.tasks[:i], 
											m.processing_tasks.tasks[i + 1:]...)
		}
	}
	return nil
}


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
	//l, e := net.Listen("tcp", ":1234")
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
	return m.finished
}

// check if we need reissue the task
func (m *Master) check() {
	for {
		m.processing_tasks.lock.Lock()
		if len(m.processing_queue) == 0 {
			m.processing_tasks.lock.Unlock()
			time.Sleep(time.Second)
			continue
		}
		m.processing_tasks.lock.Unlock()

		item := m.processing_queue.Pop().(*Item)

		if time.Now().Unix() - item.priority > m.reissue_delta {
			find := false
			for _, done_task:= range m.done_tasks.tasks {
				if done_task.Id == item.task.Id {
					find = true
				}
			}

			if (!find) {
				// log.Println("Need reissue task")
				m.wait_tasks.tasks = append(m.wait_tasks.tasks, item.task)
				continue
			}
		} else {
			m.processing_queue.Push(item)

			// just wait for timeout
			time.Sleep(time.Second)
		}
		// log.Println("Wait for reissue...")
	}
}

// start a thread for process reduce info
func (m *Master) wait_to_reduce(nReduce int, start_task_id int) {
	for len(m.wait_tasks.tasks) > 0 || len(m.processing_tasks.tasks) > 0 {
		time.Sleep(time.Second)
	}

	// log.Println("All map tasks done")
	// clear priority queue
	m.processing_queue = PriorityQueue{}

	// All map tasks have been finished
	for i := 0; i < nReduce; i++ {
		task := Task {Id: start_task_id + i, Hash_key: i, Task_type: ReduceTask}
		m.wait_tasks.tasks = append(m.wait_tasks.tasks, &task)
	}

	for len(m.wait_tasks.tasks) > 0 || len(m.processing_tasks.tasks) > 0 {
		time.Sleep(time.Second)
	}
	// log.Println("All reduce tasks done")
	m.finished = true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{reissue_delta: 10, nreduce: nReduce}

	task_id := 0;
	for _, filename := range files {
		file, err := os.Open(filename);
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		task := Task {Id: task_id, Content: content, Task_type: MapTask, Filename: filename}
		task_id++

		m.wait_tasks.tasks = append(m.wait_tasks.tasks, &task)
	}

	// log.Printf("All map task number: %v", len(m.wait_tasks.tasks))

	m.server()
	go m.wait_to_reduce(nReduce, task_id)
	go m.check()

	return &m
}
