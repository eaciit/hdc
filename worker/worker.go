package worker

import (
	"fmt"
)

type Manager struct {
	FreeWorkers chan *Worker
	Tasks       chan func()
	Done        chan bool
}

type Worker struct {
	WorkerId    int
	FreeWorkers chan *Worker
}

func NewManager(totalworker int, timeout int64) Manager {
	m := Manager{}
	m.FreeWorkers = make(chan *Worker, totalworker)
	m.Tasks = make(chan func())
	m.Done = make(chan bool, 1)

	return m
}

func (m *Manager) DoMonitor() {
	for {
		select {
		case task := <-m.Tasks:
			m.Dispatch(task)
		case <-m.Done:
			m.Done <- true
			return
		}
	}
}

func (m *Manager) Dispatch(task func()) {
	select {
	case worker := <-m.FreeWorkers:
		fmt.Println("Worker nganggur ", worker.WorkerId)
		worker.Work(task)
	case <-m.Done:
		m.Done <- true
		return
	}
}

func (w *Worker) Work(task func()) {
	task()
	w.FreeWorkers <- w
}
