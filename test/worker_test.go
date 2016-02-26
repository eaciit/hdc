package worker

import (
	"bufio"
	"fmt"
	_ "github.com/eaciit/hdc/hive"
	"os"
	"sync"
	"testing"
)

// test worker
func TestWorker(t *testing.T) {
	h := HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@", "")

	var wg sync.WaitGroup
	file, _ := os.Open("worker_test.txt")
	defer file.Close()
	scanner := bufio.NewScanner(file)

	// initialize manager and workers
	totalworker := 100
	manager := NewHiveManager(totalworker)
	for i := 0; i < totalworker; i++ {
		manager.FreeWorkers <- &w.Worker{i, manager.TimeProcess, manager.FreeWorkers, h, false}
	}

	// monitoring worker thats free
	wg.Add(1)
	go manager.DoMonitor(&wg)

	// reading file
	for scanner.Scan() {
		// getting data per line
		data := scanner.Text()

		// send task to free worker
		manager.Tasks <- data
	}

	// waiting for tasks has been done
	wg.Add(1)
	go manager.Timeout(1, &wg)
	<-manager.Done

	fwk := <-manager.FreeWorkers
	fwk.Conn.Close()
}
