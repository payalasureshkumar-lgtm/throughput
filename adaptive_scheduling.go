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
	MemoryTotal int64
	MemoryUsed  atomic.Int64
	ActiveTasks atomic.Int64
	Queue       chan Task
}

type Scheduler struct {
	Nodes []*Node
}

func NewNodes(count int) []*Node {
	nodes := make([]*Node, count)

	for i := 0; i < count; i++ {
		nodes[i] = &Node{
			ID:          i + 1,
			CPUCapacity: 1.0 + float64(i%3)*0.25,
			MemoryTotal: 4096,
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

func (s *Scheduler) AdaptiveSchedule(task Task) {
	var selected *Node
	bestScore := -1.0

	for _, node := range s.Nodes {
		availableMemory := node.MemoryTotal - node.MemoryUsed.Load()

		if availableMemory < task.MemoryMB {
			continue
		}

		activeTasks := float64(node.ActiveTasks.Load())
		loadFactor := 1.0 / (1.0 + activeTasks/node.CPUCapacity)
		memoryFactor := float64(availableMemory) / float64(node.MemoryTotal)

		score := (0.7 * loadFactor) + (0.3 * memoryFactor)

		if score > bestScore {
			bestScore = score
			selected = node
		}
	}

	if selected == nil {
		selected = s.Nodes[0]

		for _, node := range s.Nodes {
			if node.MemoryUsed.Load() < selected.MemoryUsed.Load() {
				selected = node
			}
		}
	}

	selected.ActiveTasks.Add(1)
	selected.MemoryUsed.Add(task.MemoryMB)
	selected.Queue <- task
}

func Worker(node *Node, completed *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()

	for task := range node.Queue {
		duration := time.Duration(
			float64(task.WorkUnits) /
				node.CPUCapacity *
				float64(10*time.Millisecond),
		)

		time.Sleep(duration)

		node.MemoryUsed.Add(-task.MemoryMB)
		node.ActiveTasks.Add(-1)
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
		scheduler.AdaptiveSchedule(task)
	}

	for _, node := range nodes {
		close(node.Queue)
	}

	wg.Wait()

	elapsed := time.Since(start).Seconds()
	throughput := float64(completed.Load()) / elapsed

	fmt.Println("Scheduling Algorithm: Adaptive Resource-Aware")
	fmt.Printf("Cluster Size: %d nodes\n", nodeCount)
	fmt.Printf("Total Tasks: %d\n", taskCount)
	fmt.Printf("Completed Tasks: %d\n", completed.Load())
	fmt.Printf("Execution Time: %.4f seconds\n", elapsed)
	fmt.Printf("Adaptive Throughput: %.2f tasks/sec\n", throughput)
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
