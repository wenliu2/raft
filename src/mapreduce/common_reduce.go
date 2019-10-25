package mapreduce

import (
	//"fmt"
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//fmt.Printf("doReduce: jobName: %s, reduceTask: %d, outFile: %s, nMap: %d\n", jobName, reduceTask, outFile, nMap)

	var kvs []KeyValue
	for idx := 0; idx < nMap; idx++ {
		fileName := reduceName(jobName, idx, reduceTask)
		//fmt.Printf("doReduce: reading fileName: %s\n", fileName)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatal(err)
			}
			//fmt.Printf("kv: %v\n", kv)
			kvs = append(kvs, kv)
		}
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key > kvs[j].Key
	})

	out, err := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	var key string
	isFirst := true
	var values []string
	for _, kv := range kvs {
		if isFirst {
			isFirst = false
			key = kv.Key
			values = append(values, kv.Value)
		} else if kv.Key == key {
			values = append(values, kv.Value)
		} else {
			err := enc.Encode(KeyValue{key, reduceF(key, values)})
			if err != nil {
				log.Fatal(err)
			}
			key = kv.Key
			values = []string{kv.Value}
		}
	}
	err = enc.Encode(KeyValue{key, reduceF(key, values)})
	if err != nil {
		log.Fatal(err)
	}

	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
