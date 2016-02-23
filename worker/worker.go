package worker

import (
	"time"
)

// manager model
type Manager struct {
	FreeWorkers  chan *Worker
	Tasks        chan func()
	Done         chan bool
	TimeProcess  chan int64
	LastProcess  int64
	TotalTimeOut int64
}

// worker model
type Worker struct {
	WorkerId    int
	TimeProcess chan int64
	FreeWorkers chan *Worker
}

// initiate new manager
func NewManager(numWorkers int) Manager {
	var totaltimeout int64 = 10

	m := Manager{}
	m.FreeWorkers = make(chan *Worker, numWorkers)
	m.Tasks = make(chan func())
	m.TimeProcess = make(chan int64)
	m.TotalTimeOut = totaltimeout
	m.LastProcess = time.Now().Unix()
	m.Done = make(chan bool, 1)
	return m
}

// do monitoring worker thats free or not
func (m *Manager) DoMonitor() {
	for {
		select {
		case task := <-m.Tasks:
			go m.AssignTask(task)
		case result := <-m.TimeProcess:
			go m.InProgress(result)
		case <-m.Done:
			m.Done <- true
			return
		}
	}
}

// assign task to free worker
func (m *Manager) AssignTask(task func()) {
	select {
	case worker := <-m.FreeWorkers:
		go worker.Work(task)
	case isDone := <-m.Done:
		m.Done <- isDone
		return
	}
}

// check if a task still in progress to wait it till finish
func (m *Manager) InProgress(result int64) {
	m.LastProcess = int64(result)
}

// set the timeout to waiting for tasks execution
func (m *Manager) Timeout(seconds int) {
	for {
		if time.Now().Unix()-m.LastProcess > int64(seconds) {
			m.Done <- true
			return
		} else {
			time.Sleep(time.Millisecond)
		}
	}
}

// do a task for worker
func (w *Worker) Work(task func()) {
	task()
	w.TimeProcess <- time.Now().Unix()
	w.FreeWorkers <- w
}
