package main

import (
	"flag"
	"log"
	. "MapReduce/mrUtil"
	. "MapReduce/master"
)
 var (
	configFile *string
	util *MapReduceUtil
 )

type MapReduceUtil struct {
	spec *MapReduceSpec;
}

func (mrUtil *MapReduceUtil) ReadConfigurations(filePath *string) bool {
	return LoadConfig(filePath, mrUtil.spec);
}

func (mrUtil *MapReduceUtil) RunMaster() bool {
	masterNode := &Master{MRSpec: mrUtil.spec};
	return masterNode.Run();
}

func (mrUtil *MapReduceUtil) Run(filePath *string) bool {
	mrUtil.spec = new(MapReduceSpec);

	if !mrUtil.ReadConfigurations(filePath) {
		log.Fatalf("failed to read configurations");
		return false;
	}

	if !mrUtil.RunMaster() {
		log.Fatalf("failed to start master");
		return false;
	}

	return false;
}

func init() {
	configFile = flag.String("config", "", "path to config file");
	flag.Parse();

	if len(*configFile) == 0 {
		log.Fatalf("Pass valid config file path as argument");
		return;
	}

	util = &MapReduceUtil{};
}
func main() {
	
	util.Run(configFile);
	return;
}