package lib

type ThreadPool struct {
	nWorkers int
	result chan bool
}

func (tp *ThreadPool) Init(nWorkers int) {
	tp.nWorkers = nWorkers;
	tp.result = make(chan bool, nWorkers);
}

func (tp *ThreadPool) AddResult(result bool) {
	tp.result <- result;
}

func (tp *ThreadPool) GetResult() bool {
	return <-tp.result
}

func (tp *ThreadPool) Wait() {
	for i:=0; i<tp.nWorkers; i++ {
		<-tp.result;
	}
}
func (tp *ThreadPool) Close() {
	close(tp.result);
}