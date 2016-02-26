package hive

import (
	"log"
	"strings"
	"sync"
	"time"
)

// manager model
type HiveManager struct {
	FreeWorkers  chan *HiveWorker
	Tasks        chan string
	Done         chan bool
	TimeProcess  chan int64
	LastProcess  int64
	TotalTimeOut int64
}

// worker model
type HiveWorker struct {
	WorkerId    int
	TimeProcess chan int64
	FreeWorkers chan *HiveWorker
	Context     *Hive
}

// initiate new manager
func NewHiveManager(numWorkers int) HiveManager {
	var totaltimeout int64 = 10

	m := HiveManager{}
	m.FreeWorkers = make(chan *HiveWorker, numWorkers)
	m.Tasks = make(chan string)
	m.TimeProcess = make(chan int64)
	m.TotalTimeOut = totaltimeout
	m.LastProcess = time.Now().Unix()
	m.Done = make(chan bool, 1)
	return m
}

// do monitoring worker thats free or not
func (m *HiveManager) DoMonitor(wg *sync.WaitGroup) {
	for {
		select {
		case task := <-m.Tasks:
			log.Println("Preparing do task", task.(string))
			wg.Add(1)
			go m.AssignTask(task, wg)
		case result := <-m.TimeProcess:
			wg.Add(1)
			go m.InProgress(result, wg)
		case <-m.Done:
			m.Done <- true
			m.EndWorker()
			return
		}
	}
	wg.Wait()
}

// assign task to free worker
func (m *HiveManager) AssignTask(task string, wg *sync.WaitGroup) {
	defer wg.Done()
	select {
	case worker := <-m.FreeWorkers:
		log.Println("Assign task to worker", worker.WorkerId)
		wg.Add(1)
		go worker.Work(task, wg)
	case isDone := <-m.Done:
		m.Done <- isDone
		return
	}
}

// check if a task still in progress to wait it till finish
func (m *HiveManager) InProgress(result int64, wg *sync.WaitGroup) {
	defer wg.Done()
	m.LastProcess = int64(result)
}

// set the timeout to waiting for tasks execution
func (m *HiveManager) Timeout(seconds int, wg *sync.WaitGroup) {
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

func (m *HiveManager) EndWorker() {
	for {
		select {
		case worker := <-m.FreeWorkers:
			worker.Context.Conn.Close()
		default:
			return
		}
	}
}

// do a task for worker
func (w *HiveWorker) Work(task string, wg *sync.WaitGroup) {
	defer wg.Done()

	if err := w.Context.Conn.TestConnection(); err != nil {
		w.Context.Conn.Open()
	}

	log.Println("Do task ", task)
	query := task
	if strings.LastIndex(query, ";") == -1 {
		query += ";"
	}
	w.Context.Conn.SendInput(query)

	w.TimeProcess <- time.Now().Unix()
	w.FreeWorkers <- w
}
