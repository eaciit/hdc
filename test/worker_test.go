package worker

import (
	"bufio"
	"fmt"
	_ "github.com/masmeka/hdc/hive"
	w "github.com/masmeka/hdc/worker"
	"os"
	"sync"
	"testing"
)

// test worker
func TestWorker(t *testing.T) {
	ctx := Context{&dctx}

	var wg sync.WaitGroup
	file, _ := os.Open("worker_test.txt")
	defer file.Close()
	scanner := bufio.NewScanner(file)

	// initialize manager and workers
	totalworker := 100
	manager := w.NewManager(totalworker)
	for i := 0; i < totalworker; i++ {
		manager.FreeWorkers <- &w.Worker{i, manager.TimeProcess, manager.FreeWorkers, ctx}
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
