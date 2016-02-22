package worker

import (
	"bufio"
	"fmt"
	w "github.com/eaciit/hdc/worker"
	"os"
	"testing"
)

// test worker
func TestWorker(t *testing.T) {
	file, _ := os.Open("worker_test.txt")
	defer file.Close()
	scanner := bufio.NewScanner(file)

	// initialize manager and workers
	totalworker := 1
	manager := w.NewManager(totalworker, 2)
	for i := 0; i < totalworker; i++ {
		manager.FreeWorkers <- &w.Worker{i, manager.TimeProcess, manager.FreeWorkers}
	}

	// monitoring worker thats free
	go manager.DoMonitor()

	// reading file
	for scanner.Scan() {
		// getting data per line
		data := scanner.Text()

		// send task to free worker
		manager.Tasks <- func() {
			// do something here
			fmt.Println(data)
		}
	}

	// waiting for tasks has been done
	go manager.Timeout(1)
	<-manager.Done
}
