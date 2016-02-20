package hive

import (
	"bufio"
	"errors"
	// "fmt"
	"github.com/eaciit/errorlib"
	"io"
	"log"
	"os/exec"
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

var hr *HiveResult

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

		if d.FnReceive != nil {
			go func() {
				_, e = d.Wait()
			}()
		}
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

func (d *DuplexTerm) SendInput(input string) (result []string, e error) {
	iwrite, e := d.Writer.WriteString(input + "\n")
	if iwrite == 0 {
		e = errors.New("Writing only 0 byte")
	} else {
		e = d.Writer.Flush()
	}

	if e != nil {
		return
	}

	if d.FnReceive == nil {
		result, e = d.Wait()
	}

	return
}

func (d *DuplexTerm) Wait() (result []string, e error) {
	isHeader := false
	for {
		peekBefore, _ := d.Reader.Peek(14)
		peekBeforeStr := string(peekBefore)
		// log.Printf("peekBefore: %v\n", peekBefore)
		// log.Printf("peekBeforeStr: %v\n", peekBeforeStr)

		bread, e := d.Reader.ReadString('\n')
		bread = strings.TrimRight(bread, "\n")

		peek, _ := d.Reader.Peek(14)
		peekStr := string(peek)
		// log.Printf("peek: %v\n", peek)
		// log.Printf("peekStr: %v\n", peekStr)

		delimiter := "\t"

		if d.OutputType == CSV {
			delimiter = ","
		}

		if isHeader {
			hr := HiveResult{}
			hr.constructHeader(bread, delimiter)
			log.Printf("model: %v\n", hr)
			log.Printf("headerStr: %v\n", bread)
			for _, val := range hr.Header {
				log.Printf("header: %v\n", val)
			}

			isHeader = false
		}

		if BEE_CLI_STR == peekBeforeStr {
			isHeader = true
		}

		if !strings.Contains(bread, BEE_CLI_STR) {
			//result = append(result, bread)
			if d.FnReceive != nil {
				/*Parse(hr.Header, bread, hr.ResultObj, d.OutputType, d.DateFormat)
				log.Printf("model: %v\n", hr.ResultObj)*/
				d.FnReceive(bread)
			} else {
				result = append(result, bread)
			}
		}

		if d.FnReceive != nil {
			if (e != nil && e.Error() == "EOF") || (strings.Contains(peekStr, CLOSE_SCRIPT)) {
				break
			}
		} else {
			if (e != nil && e.Error() == "EOF") || (BEE_CLI_STR == peekStr) {
				break
			}
		}

	}

	return
}
