package worker

import (
	"fmt"
	w "github.com/masmeka/hdc/worker"
	"testing"
)

func TestWorker(t *testing.T) {
	// data testing
	arrData := []string{"data 1", "data 2", "data 3", "data 4", "data 5", "data 6", "data 7", "data 8"}

	// define totalworker
	totalWorker := 4

	// init manager
	manager := w.NewManager(totalWorker, 3)

	// define free workers
	for i := 0; i < totalWorker; i++ {
		manager.FreeWorkers <- &w.Worker{i, manager.FreeWorkers}
	}

	// monitoring workers
	go manager.DoMonitor()

	// get tasks
	for x := range arrData {
		manager.Tasks <- func() {
			defer fmt.Println("Do task ", x)
		}
	}

	fmt.Println("Work Done!")
}
