package mrUtil;

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)


type MapReduceSpec struct {
	K int64;
	Workers []string;
	InputPath string;
	OutputPath string; 
}

func ParseValues(line string, delim byte) []string {
	res := []string{};
	buffer := bytes.NewBufferString("");
	for i := 0; i < len(line); i++ {
		if line[i] != delim {
			buffer.WriteByte(line[i]);
		}
		if line[i] == delim && buffer.Len() != 0 {
			res = append(res, buffer.String());
			buffer = bytes.NewBufferString("");
		}
	}
	if buffer.Len() > 0 {
		res = append(res, buffer.String());
	}
	return res;
}


func LoadConfig(filePath *string, spec *MapReduceSpec) bool {
	file, err := os.Open(*filePath);
	if err != nil {
		log.Fatalf("failed to open file");
		return false;
	}
	defer file.Close();

	scanner := bufio.NewScanner(file);
	for scanner.Scan() {
	 	line := scanner.Text(); // the line
		if strings.Contains(line, "K") {
			value := strings.Split(line, "=")[1];
			spec.K, err = strconv.ParseInt(value, 10, 64);
			fmt.Printf("K: %v\n", spec.K);
		} else if strings.Contains(line, "workers") {
			value := strings.Split(line, "=")[1];
			spec.Workers = ParseValues(value, ';');
			fmt.Printf("worker: %v\n", spec.Workers[0]);
		} else if strings.Contains(line, "inputPath") {
			value := strings.Split(line, "=")[1];
			spec.InputPath = value;
			fmt.Printf("ip path: %v\n", spec.InputPath);
		} else if strings.Contains(line, "outputPath") {
			value := strings.Split(line, "=")[1];
			spec.OutputPath = value;
			fmt.Printf("op path: %v\n", spec.OutputPath);
		} else {
			log.Fatalf("invalid input");
			return false;
		}
	}

	return true;
}



