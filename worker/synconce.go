package worker

// import (
// 	"fmt"
// 	"time"
// )

// type Manager struct {
// 	In      chan func()
// 	done    chan error
// 	timeout chan struct{}
// }

// func NewManager(workers int, timeout time.Duration) *Manager {
// 	m := &Manager{
// 		In:      make(chan func(), workers),
// 		done:    make(chan error, workers),
// 		timeout: make(chan struct{}),
// 	}
// 	time.AfterFunc(timeout, func() {
// 		close(m.timeout)
// 	})
// 	for w := 0; w < workers; w++ {
// 		go func() {
// 			for {
// 				select {
// 				case f, ok := <-m.In:
// 					if !ok {
// 						m.done <- nil
// 						return
// 					}
// 					f()
// 				case <-m.timeout:
// 					m.done <- fmt.Errorf("timed out")
// 				}
// 			}
// 		}()
// 	}
// 	return m
// }

// func (m *Manager) Wait() error {
// 	close(m.In)
// 	var err error
// 	for w := 0; w < cap(m.done); w++ {
// 		err1 := <-m.done
// 		if err1 != nil {
// 			err = err1
// 		}
// 	}
// 	return err
// }

// func main() {
// 	m := NewManager(6, 3*time.Second)
// 	for i := 0; i < 10; i++ {
// 		ii := i
// 		m.In <- func() {
// 			// Do whatever you want here, send results where you need them
// 			fmt.Printf("working on task %v at %v\n", ii, time.Now())
// 			time.Sleep(time.Second)
// 		}
// 	}
// 	if err := m.Wait(); err != nil {
// 		fmt.Printf("Wait error: %v\n", err)
// 	}
// }
