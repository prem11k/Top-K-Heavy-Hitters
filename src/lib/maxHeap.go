package lib

import "fmt"

type Node struct {
	Key string
	Value int64
};

type MinHeap []Node;

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].Value < h[j].Value }
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(Node))
}

func (h *MinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h MinHeap) Print() {
	for i:=0; i<h.Len(); i++ {
		fmt.Printf("%v: %v; ", h[i].Key, h[i].Value);
	}
	fmt.Printf("\n");
	return;
}