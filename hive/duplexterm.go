package hive

import (
	"bufio"
	"errors"
	"github.com/eaciit/errorlib"
	"io"
	"os/exec"
	"strings"
)

const (
	BEE_CLI_STR  = "jdbc:hive2:"
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

	d.FnReceive = nil
	d.Cmd.Wait()
	d.Stdin.Close()
	d.Stdout.Close()
}

func (d *DuplexTerm) SendInput(input string) (res []string, err error) {
	if d.FnReceive != nil {
		done := make(chan bool)
		go func() {
			_, e, _ := d.process()
			_ = e
			done <- true
		}()
		iwrite, e := d.Writer.WriteString(input + "\n")
		if iwrite == 0 {
			e = errors.New("Writing only 0 byte")
		} else {
			e = d.Writer.Flush()
		}

		<-done
		d.FnReceive = nil
		err = e
	} else {
		iwrite, e := d.Writer.WriteString(input + "\n")
		if iwrite == 0 {
			e = errors.New("Writing only 0 byte")
		} else {
			e = d.Writer.Flush()
		}
		if e == nil && d.FnReceive == nil {
			done := make(chan bool)
			go func() {
				res, e, _ = d.process()
				done <- true
			}()
			<-done
		}
		err = e
	}
	return
}

func (d *DuplexTerm) process() (result []string, e error, status bool) {
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
			hr = HiveResult{}
			hr.constructHeader(bread, delimiter)
			isHeader = false
		} else if !strings.Contains(bread, BEE_CLI_STR) {
			if d.FnReceive != nil {

				hr.Result = append(hr.Result, bread)
				Parse(hr.Header, bread, &hr.ResultObj, d.OutputType, d.DateFormat)
				d.FnReceive(hr)
			} else {
				result = append(result, bread)
			}
		}

		if d.FnReceive != nil && strings.Contains(peekBeforeStr, BEE_CLI_STR) {
			isHeader = true
		}
		if (e != nil && e.Error() == "EOF") || strings.Contains(peekStr, BEE_CLI_STR) {
			break
		}

	}

	return
}
