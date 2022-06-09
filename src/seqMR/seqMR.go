package seqMR

import (
	"bufio"
	"container/heap"
	"fmt"
	"strings"
	. "MapReduce/lib"
	"os"
	"path/filepath"
	"strconv"
)

type Request struct {
	K           int64  
	RequestType string 
	UserId      string 
	InputPath   string
	OutputPath  string 
}

func mapper(req *Request) (string, error) {
	file, err := os.Open(req.InputPath);
	if err != nil {
		panic("Failed to open input file: "+req.InputPath);
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

	outputFile := filepath.Join(req.OutputPath, req.UserId+"-mapper.txt");
	file, err = os.Create(outputFile);
	if err != nil {
		panic("Failed to open outputfile: "+outputFile);
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
	mapOutputFile := req.OutputPath;
	minHeap := MinHeap{};
	heap.Init(&minHeap);

	file, err := os.Open(mapOutputFile);
	if err != nil {
		panic("Failed to open input file: "+req.InputPath);
		//return make(map[string]int64), err;
	}

	scanner := bufio.NewScanner(file);
	for scanner.Scan() {
		line := scanner.Text();
		keyValue := strings.Split(line, " : ");
		value, _ := strconv.ParseInt(keyValue[1], 10, 64)
		heap.Push(&minHeap, Node{Key: keyValue[0], Value: value});
		if(minHeap.Len() > int(req.K)) {
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
	inputPath := "test/input/inputs.txt";

	req := &Request{};
	req.K = 3;
	req.RequestType = "Map"
	req.UserId = "test"
	req.InputPath = inputPath;
	req.OutputPath = "test/output";

	outputPath, err := mapper(req);
	if err != nil {
		panic("sequential map failed");
	}
	req.OutputPath = outputPath;
	req.RequestType = "Reduce";

	freqMap, err := reducer(req);
	if err != nil {
		panic("sequential reduce failed");
	}

	fmt.Printf("Top K items for K=%v\n", req.K);
	for key, value := range freqMap {
		fmt.Printf("%v : %v\n", key, value);
	}
	return;
}