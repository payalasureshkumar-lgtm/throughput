package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	ID        int
	WorkUnits int
	MemoryMB  int64
}

type Node struct {
	ID          int
	CPUCapacity float64
	Queue       chan Task
}

type Scheduler struct {
	Nodes    []*Node
	NextNode int
}

func NewNodes(count int) []*Node {
	nodes := make([]*Node, count)

	for i := 0; i < count; i++ {
		nodes[i] = &Node{
			ID:          i + 1,
			CPUCapacity: 1.0,
			Queue:       make(chan Task, 10000),
		}
	}

	return nodes
}

func NewTasks(count int) []Task {
	rng := rand.New(rand.NewSource(42))
	tasks := make([]Task, count)

	for i := 0; i < count; i++ {
		tasks[i] = Task{
			ID:        i + 1,
			WorkUnits: rng.Intn(5) + 1,
			MemoryMB:  int64(rng.Intn(256) + 128),
		}
	}

	return tasks
}

func (s *Scheduler) RoundRobin(task Task) {
	node := s.Nodes[s.NextNode%len(s.Nodes)]
	s.NextNode++
	node.Queue <- task
}

func Worker(node *Node, completed *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()

	for task := range node.Queue {
		duration := time.Duration(task.WorkUnits) * 10 * time.Millisecond
		time.Sleep(duration)
		completed.Add(1)
	}
}

func Run(nodeCount, taskCount int) {
	nodes := NewNodes(nodeCount)
	tasks := NewTasks(taskCount)

	var completed atomic.Int64
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go Worker(node, &completed, &wg)
	}

	scheduler := Scheduler{
		Nodes: nodes,
	}

	start := time.Now()

	for _, task := range tasks {
		scheduler.RoundRobin(task)
	}

	for _, node := range nodes {
		close(node.Queue)
	}

	wg.Wait()

	elapsed := time.Since(start).Seconds()
	throughput := float64(completed.Load()) / elapsed

	fmt.Printf("Scheduling Algorithm: Round Robin\n")
	fmt.Printf("Cluster Size: %d nodes\n", nodeCount)
	fmt.Printf("Total Tasks: %d\n", taskCount)
	fmt.Printf("Completed Tasks: %d\n", completed.Load())
	fmt.Printf("Execution Time: %.4f seconds\n", elapsed)
	fmt.Printf("RR Throughput: %.2f tasks/sec\n", throughput)
}

func main() {
	nodes := flag.Int("nodes", 3, "number of computing nodes")
	tasks := flag.Int("tasks", 1000, "number of tasks")
	flag.Parse()

	if *nodes < 1 || *tasks < 1 {
		fmt.Println("Nodes and tasks must be greater than zero")
		return
	}

	Run(*nodes, *tasks)
}

