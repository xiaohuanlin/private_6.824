package mr

import "fmt"
import "log"
import "time"
import "net/rpc"
import "hash/fnv"
import "strconv"
import "io/ioutil"
import "os"
import "encoding/json"
import "sort"
import "path/filepath"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type TaskType int32
const (
	MapTask TaskType = 0 + iota
	ReduceTask
	WaitTask
)

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
	worker_id := time.Now().UnixNano()
    for {
		reply, ok := FetchTask(worker_id)
		if !ok {
			// master become offline
			return
		}

		switch reply.Task.Task_type {
			case MapTask: {
				json_contents := make(map[int][]KeyValue)
				for _, kv := range mapf(reply.Task.Filename, string(reply.Task.Content)) {
					hash_value := ihash(kv.Key) % reply.Nreduce
					json_contents[hash_value] = append(json_contents[hash_value], kv)
				}			

				for k, v := range json_contents {
					filename := "mr" + "-" + strconv.Itoa(reply.Task.Id) + "-" + strconv.Itoa(k)

					file, err := ioutil.TempFile(".", "tmp_map")
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}

					enc := json.NewEncoder(file)
					enc.Encode(v)

					file.Close()
					os.Rename(file.Name(), filename)
				}

				_, ok := AnswerTask(reply.Task.Id, worker_id)
				if !ok {
					return
				}
			}
			case ReduceTask: {
				hash_value := reply.Task.Hash_key
				pattern:= "mr-*-" + strconv.Itoa(hash_value)

				files, err := ioutil.ReadDir(".")
				if err != nil {
					log.Fatal(err)
				}

				kva := []KeyValue {}

				for _, file_info := range files {
					exist, _ := filepath.Match(pattern, file_info.Name())
					if !exist {
						continue
					}

					file, err:= os.Open(file_info.Name())
					if err != nil {
						log.Fatalf("cannot open %v", file_info.Name())
					}

					dec := json.NewDecoder(file)
					_, err = dec.Token()
					if err != nil {
						log.Fatal(err)
					}

					for dec.More() {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							log.Fatal(err)
							break
						}
						kva = append(kva, kv)
					}

					_, err = dec.Token()
					if err != nil {
						log.Fatal(err)
					}
				}
				sort.Sort(ByKey(kva))

				oname := "mr-out-" + strconv.Itoa(hash_value)
				ofile, _ := ioutil.TempFile(".", "tmp")
			
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
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
					i = j
				}
			
				ofile.Close()
				os.Rename(ofile.Name(), oname)

				_, ok := AnswerTask(reply.Task.Id, worker_id)
				if !ok {
					return
				}
			}
			default:
				time.Sleep(time.Second)
				continue
		}
	}

}


// fetch task
func FetchTask(worker_id int64) (Reply, bool) {

	task_args := Args {worker_id}
	task_reply := Reply {}

	if !call("Master.FetchTasks", &task_args, &task_reply) {
		return task_reply, false
	}
	fmt.Printf("reply task_id: %v\ntype: %v\nhash_key: %v\n", 
				task_reply.Task.Id, 
				task_reply.Task.Task_type, 
				task_reply.Task.Hash_key)
	return task_reply, true
}

func AnswerTask(task_id int, worker_id int64) (AnswerReply, bool) {
	fmt.Printf("answer reply task_id: %v\nworker_id: %v\n", task_id, worker_id)
	task_args := AnswerArgs {task_id, worker_id}
	task_reply := AnswerReply {}

	return task_reply, call("Master.ReplyTasks", &task_args, &task_reply)
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
