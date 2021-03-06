package worker

import (
	_ "github.com/eaciit/hdc/hive"
	"sync"
	"time"
)

// manager model
type Manager struct {
	FreeWorkers  chan *Worker
	Tasks        chan interface{}
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
	Context     *Hive
}

// initiate new manager
func NewManager(numWorkers int) Manager {
	var totaltimeout int64 = 10

	m := Manager{}
	m.FreeWorkers = make(chan *Worker, numWorkers)
	m.Tasks = make(chan interface{})
	m.TimeProcess = make(chan int64)
	m.TotalTimeOut = totaltimeout
	m.LastProcess = time.Now().Unix()
	m.Done = make(chan bool, 1)
	return m
}

func NewWorker(id int, timeProcess <-chan int64, freeWorkers <-chan *Worker, ctx *Hive) Worker {
	wk := Worker{}
	wk.WorkerId = id
	wk.TimeProcess <- timeProcess
	wk.FreeWorkers <- freeWorkers
	wk.Context = ctx

	wk.Context.Conn.Open()

	return wk
}

// do monitoring worker thats free or not
func (m *Manager) DoMonitor(wg *sync.WaitGroup) {
	for {
		select {
		case task := <-m.Tasks:
			wg.Add(1)
			go m.AssignTask(task, wg)
		case result := <-m.TimeProcess:
			wg.Add(1)
			go m.InProgress(result, wg)
		case <-m.Done:
			m.Done <- true
			return
		}
	}
	wg.Wait()
}

// assign task to free worker
func (m *Manager) AssignTask(task interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	select {
	case worker := <-m.FreeWorkers:
		wg.Add(1)
		go worker.Work(task, wg)
	case isDone := <-m.Done:
		m.Done <- isDone
		return
	}
}

// check if a task still in progress to wait it till finish
func (m *Manager) InProgress(result int64, wg *sync.WaitGroup) {
	defer wg.Done()
	m.LastProcess = int64(result)
}

// set the timeout to waiting for tasks execution
func (m *Manager) Timeout(seconds int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if time.Now().Unix()-m.LastProcess > int64(seconds) {
			m.Done <- true
			return
		} else {
			time.Sleep(time.Millisecond)
		}
	}
}

func (m *Manager) EndWorker() {
	for {
		select {
		case worker <- m.FreeWorkers:
			worker.Context.Conn.Close()
		default:
			return
		}
	}
}

// do a task for worker
func (w *Worker) Work(task interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	w.Context.fetch(task.(string))

	w.TimeProcess <- time.Now().Unix()
	w.FreeWorkers <- w
}
