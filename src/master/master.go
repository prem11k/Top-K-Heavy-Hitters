package master

import (
	"context"
	"container/heap"
	. "MapReduce/lib"
	. "MapReduce/mrUtil"
	. "MapReduce/generated"
	"fmt"
	"google.golang.org/grpc"
	"time"
)
type WorkerClient struct {
	UserId string;
}

type Master struct {
	MRSpec *MapReduceSpec
}

var (
	workers map[string]*WorkerClient;
	mappers map[string]*Request;
	reducers map[string]*Request;
	mapperOutput map[string]string;
	topKFrequentItems map[string]int64;
)

func (worker *WorkerClient) ScheduleWork(ctx context.Context, request *Request) (*Response, error) {
	conn, err := grpc.Dial(worker.UserId+":4040", grpc.WithInsecure());
	if err != nil {
		panic(err);
	}
	client := NewMapReduceServiceClient(conn);
	resp, err := client.DoWork(ctx, request);

	return resp, err;
}

func (master *Master) scheduleMapJob(worker string, nAttempt int) bool {

	if _, ok := workers[worker]; !ok {
		workers[worker] = &WorkerClient{UserId: worker};
	}
	workerClient := workers[worker];

	var workerRequest *Request;
	if _, ok := mappers[worker]; !ok {
		workerRequest = &Request{};
		workerRequest.K = master.MRSpec.K;
		workerRequest.RequestType = "Map";
		workerRequest.UserId = worker;
		workerRequest.InputPath = master.MRSpec.InputPath;
		workerRequest.OutputPath = master.MRSpec.OutputPath;

		mappers[worker] = workerRequest;
	}
	workerRequest = mappers[worker];

	deadline := 60*time.Second
	ctx, cancelCtx := context.WithDeadline(context.Background(), time.Now().Add(deadline));

	defer cancelCtx();

	response, err := workerClient.ScheduleWork(ctx, workerRequest);
	if err != nil || !response.Success {
		if nAttempt == 5 {
			fmt.Printf("Map failed for worker [%v].\n", worker);
			return false;
		}
		fmt.Printf("Map failed for worker [%v]. Retrying after [60] seconds...\n", worker);
		time.Sleep(1*time.Second);
		return master.scheduleMapJob(worker, nAttempt+1);
	}
	fmt.Printf("Received Output at: %v\n", response.OutputPath);
	mapperOutput[worker] = response.OutputPath;
	return true;
}

func (master *Master) scheduleReduceJob(worker string, nAttempt int) bool {

	if _, ok := mapperOutput[worker]; !ok {
		fmt.Printf("Map didn't complete on worker [%v]. Hence skipping Reduce", worker);
		return false;
	}
	if _, ok := workers[worker]; !ok {
		workers[worker] = &WorkerClient{UserId: worker};
	}
	workerClient := workers[worker];

	var workerRequest *Request;
	if _, ok := reducers[worker]; !ok {
		workerRequest = &Request{};
		workerRequest.K = master.MRSpec.K;
		workerRequest.RequestType = "Reduce";
		workerRequest.UserId = worker;
		workerRequest.InputPath = mapperOutput[worker];
		workerRequest.OutputPath = master.MRSpec.OutputPath;

		reducers[worker] = workerRequest;
	}
	workerRequest = reducers[worker];

	deadline := 60*time.Second
	ctx, cancelCtx := context.WithDeadline(context.Background(), time.Now().Add(deadline));

	defer cancelCtx();

	response, err := workerClient.ScheduleWork(ctx, workerRequest);
	if err != nil || !response.Success {
		if nAttempt == 5 {
			fmt.Printf("Reduce failed for worker [%v].\n", worker);
			return false;
		}
		fmt.Printf("Reduce failed for worker [%v]. Retrying after [60] seconds...\n", worker);
		time.Sleep(1*time.Second);
		return master.scheduleReduceJob(worker, nAttempt+1);
	}

	for key, value := range response.FrequencyMap {
		if _, ok := topKFrequentItems[key]; !ok {
			topKFrequentItems[key] = value;
		} else {
			topKFrequentItems[key] += value;
		}
	}
	return true;
}

func Init() {
	workers = make(map[string]*WorkerClient);
	mappers = make(map[string]*Request);
	reducers = make(map[string]*Request);
	mapperOutput = make(map[string]string);
	topKFrequentItems = make(map[string]int64);
}
func (master *Master) Run() bool {
	Init();

	for i:=0; i<len(master.MRSpec.Workers); i++ {
		master.scheduleMapJob(master.MRSpec.Workers[i], 0);
	}
	for i:=0; i<len(master.MRSpec.Workers); i++ {
		master.scheduleReduceJob(master.MRSpec.Workers[i], 0);
	}

	minHeap := MinHeap{};
	heap.Init(&minHeap);

	for key, value  := range topKFrequentItems {
		heap.Push(&minHeap, Node{Key: key, Value: value});
		if(minHeap.Len() > int(master.MRSpec.K)) {
			heap.Pop(&minHeap);
		}
	}

	fmt.Printf("Top [%v] frequent items across all workers:\n", master.MRSpec.K);
	for minHeap.Len() > 0 {
		node := heap.Pop(&minHeap).(Node);
		fmt.Printf("%v (%v times)\n", node.Key, node.Value);
	}
	return true;
}
