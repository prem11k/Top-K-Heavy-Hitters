package main

import (
	"bufio"
	"context"
	"container/heap"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	. "MapReduce/lib"
	. "MapReduce/generated"
	"net"
	"os"
	"path/filepath"
	"strings"
	"strconv"
)

type Worker struct {
	UnimplementedMapReduceServiceServer;
};

func (worker *Worker) DoWork(ctx context.Context, req *Request) (*Response, error) {
	if strings.Compare(req.GetRequestType(), "Map") == 0 {
		outputPath, err := mapper(req);
		if err != nil {
			fmt.Printf("Map task failed.");
			return &Response{UserId: req.UserId, Success: false}, err;
		}

		return &Response{UserId: req.UserId, Success: true, OutputPath: outputPath}, nil;
		
	}

	if strings.Compare(req.GetRequestType(), "Reduce") == 0 {
		frequencyMap, err := reducer(req);
		if err != nil {
			fmt.Printf("Reduce task failed.");
			return &Response{UserId: req.UserId, Success: false}, err;
		}

		return &Response{UserId: req.UserId, Success: true, FrequencyMap: frequencyMap}, nil;
	}
	fmt.Printf("Invalid Request");
	return &Response{Success: false}, errors.New("Invalid Request");
}

func mapper(req *Request) (string, error) {
	file, err := os.Open(req.GetInputPath());
	if err != nil {
		fmt.Printf("Failed to open input file: "+req.GetInputPath());
		return "", err;
	}

	freqMap := make(map[string]int);
	scanner := bufio.NewScanner(file);
	for scanner.Scan() {
		line := scanner.Text();
		if value, ok := freqMap[line]; ok {
			freqMap[line] = value+1;
		} else {
			freqMap[line] = 1;
		}
	}
	file.Close();

	outputFile := filepath.Join(req.GetOutputPath(), req.GetUserId()+"-mapper.txt");
	file, err = os.Create(outputFile);
	if err != nil {
		fmt.Printf("Failed to open outputfile: "+outputFile);
		return "", err;
	}

	defer file.Close();
	writer := bufio.NewWriter(file);
	for key, value := range freqMap {
		_, _ = writer.WriteString(key + " : " + strconv.Itoa(value) + "\n");
	}
	writer.Flush();

	return outputFile, nil;
}

func reducer(req *Request) (map[string]int64, error) {
	mapOutputFile := req.GetInputPath();
	fmt.Printf("Reduce - i/p: [%v]\n", mapOutputFile);
	minHeap := MinHeap{};
	heap.Init(&minHeap);

	file, err := os.Open(mapOutputFile);
	if err != nil {
		panic("Failed to open input file: "+req.GetInputPath());
		return make(map[string]int64), err;
	}

	scanner := bufio.NewScanner(file);
	for scanner.Scan() {
		line := scanner.Text();
		keyValue := strings.Split(line, " : ");
		value, _ := strconv.ParseInt(keyValue[1], 10, 64)
		heap.Push(&minHeap, Node{Key: keyValue[0], Value: value});
		if(minHeap.Len() > int(req.GetK())) {
			heap.Pop(&minHeap);
		}
		
	}
	file.Close();
	
	freqMap := make(map[string]int64);
	for minHeap.Len() > 0 {
		node := heap.Pop(&minHeap).(Node);
		freqMap[node.Key] = node.Value;
	}
	return freqMap, nil;
}

func main() {
	listener, err := net.Listen("tcp", ":4040");
	if err != nil {
		panic(err);
	}
	fmt.Printf("Listening at Port 4040...\n");
	srv := grpc.NewServer();
	RegisterMapReduceServiceServer(srv, &Worker{});
	reflection.Register(srv);

	fmt.Println("Registered the server. All good to serve!")
	if err := srv.Serve(listener); err != nil {
		panic(err);
	}
	
}