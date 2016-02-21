package hive

import (
	"bufio"
	"errors"
	// "fmt"
	"github.com/eaciit/errorlib"
	// "github.com/eaciit/toolkit"
	"io"
	// "log"
	"os/exec"
	// "reflect"
	"strings"
)

const (
	BEE_CLI_STR  = "0: jdbc:hive2:"
	CLOSE_SCRIPT = "!quit"
)

type DuplexTerm struct {
	Writer     *bufio.Writer
	Reader     *bufio.Reader
	Cmd        *exec.Cmd
	CmdStr     string
	Stdin      io.WriteCloser
	Stdout     io.ReadCloser
	FnReceive  FnHiveReceive
	OutputType string
	DateFormat string
}

/*func (d *DuplexTerm) Open() (e error) {
	if d.Stdin, e = d.Cmd.StdinPipe(); e != nil {
		return
	}

	if d.Stdout, e = d.Cmd.StdoutPipe(); e != nil {
		return
	}

	d.Writer = bufio.NewWriter(d.Stdin)
	d.Reader = bufio.NewReader(d.Stdout)

	e = d.Cmd.Start()
	return
}*/

var hr HiveResult

func (d *DuplexTerm) Open() (e error) {
	if d.CmdStr != "" {
		arg := append([]string{"-c"}, d.CmdStr)
		d.Cmd = exec.Command("sh", arg...)

		if d.Stdin, e = d.Cmd.StdinPipe(); e != nil {
			return
		}

		if d.Stdout, e = d.Cmd.StdoutPipe(); e != nil {
			return
		}

		d.Writer = bufio.NewWriter(d.Stdin)
		d.Reader = bufio.NewReader(d.Stdout)

		e = d.Cmd.Start()
	} else {
		errorlib.Error("", "", "Open", "The Connection Config not Set")
	}

	return
}

func (d *DuplexTerm) Close() {
	result, e := d.SendInput(CLOSE_SCRIPT)

	_ = result
	_ = e

	d.Cmd.Wait()
	d.Stdin.Close()
	d.Stdout.Close()
}

func (d *DuplexTerm) SendInput(input string) ([]string, error) {
	done := make(chan bool)
	if d.FnReceive != nil {
		go func() {
			_, e, status := d.Wait()
			_ = e
			if status {
				done <- status
			}
		}()

		iwrite, e := d.Writer.WriteString(input + "\n")
		if iwrite == 0 {
			e = errors.New("Writing only 0 byte")
		} else {
			e = d.Writer.Flush()
		}

		<-done

		return nil, e
	} else {
		iwrite, e := d.Writer.WriteString(input + "\n")
		if iwrite == 0 {
			e = errors.New("Writing only 0 byte")
		} else {
			e = d.Writer.Flush()
		}

		if e != nil {
			return nil, e
		} else {
			result, e, status := d.Wait()
			_ = result
			_ = status

			return result, e
		}
	}
}

/*func (d *DuplexTerm) SetFn(f interface{}) {
	fn := reflect.ValueOf(f)
	fnType := fn.Type()
	if fnType.Kind() != reflect.Func || fnType.NumIn() != 1 || fnType.NumOut() != 1 {
		panic("Expected a unary function returning a single value")
	}

	d.Fn = f
}*/

func (d *DuplexTerm) Wait() (result []string, e error, status bool) {
	isHeader := false
	for {
		peekBefore, _ := d.Reader.Peek(14)
		peekBeforeStr := string(peekBefore)

		bread, e := d.Reader.ReadString('\n')
		bread = strings.TrimRight(bread, "\n")

		peek, _ := d.Reader.Peek(14)
		peekStr := string(peek)

		delimiter := "\t"

		if d.OutputType == CSV {
			delimiter = ","
		}

		if isHeader {
			hr.constructHeader(bread, delimiter)
			isHeader = false
		} else if !strings.Contains(bread, BEE_CLI_STR) {
			if d.FnReceive != nil {
				/*fn := reflect.ValueOf(d.Fn)
				// tp := fn.Type().In(0)
				// tmp := reflect.New(tp).Elem()

				Parse(hr.Header, bread, &hr.ResultObj, d.OutputType, d.DateFormat)
				log.Printf("tmp: %v\n", &hr.ResultObj)

				res := fn.Call([]reflect.Value{reflect.ValueOf(hr.ResultObj)})
				log.Printf("res: %v\n", res)*/

				/*fn := reflect.ValueOf(d.Fn)
				tp := fn.Type().In(0)
				tmp := reflect.New(tp)

				xTmp := toolkit.M{}

				Parse(hr.Header, bread, &xTmp, d.OutputType, d.DateFormat)
				log.Printf("tmp: %v\n", xTmp)*/
				// log.Printf("tmp: %v\n", tmp)

				/*res := fn.Call([]reflect.Value{reflect.ValueOf(hr.ResultObj)})
				log.Printf("test: %v\n", res)
				d.FnReceive(res)*/

				hr.Result = append(hr.Result, bread)
				Parse(hr.Header, bread, &hr.ResultObj, d.OutputType, d.DateFormat)
				d.FnReceive(hr)
			} else {
				result = append(result, bread)
			}
		}

		if d.FnReceive != nil && BEE_CLI_STR == peekBeforeStr {
			isHeader = true
		}

		/*if d.FnReceive != nil {
			if (e != nil && e.Error() == "EOF") || (strings.Contains(peekStr, CLOSE_SCRIPT)) {
				break
			}
		} else {*/
		if (e != nil && e.Error() == "EOF") || (BEE_CLI_STR == peekStr) {
			status = true
			break
		}
		// }

	}

	return
}
