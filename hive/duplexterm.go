package hive

import (
	"bufio"
	"errors"
	// "fmt"
	"io"
	// "log"
	"os/exec"
	"strings"
)

const (
	BEE_CLI_STR  = "0: jdbc:hive2:"
	CLOSE_SCRIPT = "!quit"
)

type DuplexTerm struct {
	Writer    *bufio.Writer
	Reader    *bufio.Reader
	Cmd       *exec.Cmd
	Stdin     io.WriteCloser
	Stdout    io.ReadCloser
	FnReceive FnHiveReceive
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

func (d *DuplexTerm) Open() (e error) {
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
			for {
				bread, e := d.Reader.ReadString('\n')

				peek, _ := d.Reader.Peek(14)
				peekStr := string(peek)

				if !strings.Contains(bread, BEE_CLI_STR) {
					//result = append(result, bread)
					d.FnReceive(bread)
				}

				if (e != nil && e.Error() == "EOF") || (strings.Contains(peekStr, CLOSE_SCRIPT)) {
					break
				}

			}
		}()
	}
	e = d.Cmd.Start()
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
		for {
			bread, e := d.Reader.ReadString('\n')
			peek, _ := d.Reader.Peek(14)
			peekStr := string(peek)

			if !strings.Contains(bread, BEE_CLI_STR) {
				result = append(result, bread)
			}

			if (e != nil && e.Error() == "EOF") || (BEE_CLI_STR == peekStr) {
				break
			}
		}
	}

	return
}
