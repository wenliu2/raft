package mapreduce

import "fmt"
import "sync"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	taskChan := make(chan int, ntasks)

	var wg sync.WaitGroup
	for idx := 0; idx < ntasks; idx++ {
		taskChan <- idx
		wg.Add(1)
	}

	go func() {
		for idx := range taskChan {
			// -1 means end, no more task
			if idx == -1 {
				break
			}
			file := mapFiles[idx]
			s := <-registerChan
			go func(worker string, n int) {
				args := DoTaskArgs{}
				args.Phase = phase
				args.TaskNumber = n
				args.File = file
				args.NumOtherPhase = n_other
				args.JobName = jobName
				call_ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				if call_ok {
					wg.Done()
				} else {
					// if task failed, send the task to channel for retry.
					go func() { taskChan <- n }()
				}
				//free the worker
				go func() { registerChan <- worker }()
			}(s, idx)
		}
	}() // end of go

	wg.Wait()
	taskChan <- -1

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
