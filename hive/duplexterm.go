package hive

import (
	"bufio"
	"errors"
	// "fmt"
	"io"
	"os/exec"
	"strings"
)

const (
	BEE_CLI_STR = "0: jdbc:hive2:"
)

type DuplexTerm struct {
	Writer *bufio.Writer
	Reader *bufio.Reader
	Cmd    *exec.Cmd
	Stdin  io.WriteCloser
	Stdout io.ReadCloser
}

func (d *DuplexTerm) Open() (e error) {
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
}

func (d *DuplexTerm) Close() {
	result, e := d.SendInput("!quit")

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

	for {
		bread, e := d.Reader.ReadString('\n')
		peek, _ := d.Reader.Peek(14)
		peekStr := string(peek)

		if (e != nil && e.Error() == "EOF") || (BEE_CLI_STR == peekStr) {
			break
		}

		if !strings.Contains(bread, BEE_CLI_STR) {
			result = append(result, bread)
		}
	}

	return
}

/*func main() {
	dup := DuplexTerm{}
	err := dup.Open()

	result, err := dup.SendInput("select * from sample_07 limit 5;")
	fmt.Printf("result: %v\n", result)
	// fmt.Printf("error: %v\n", err)

	result, err = dup.SendInput("select * from sample_07 limit 5;")
	fmt.Printf("result: %v\n", result)
	// fmt.Printf("error: %v\n", err)

	result, err = dup.SendInput("!quit")
	fmt.Printf("result: %v\n", result)
	// fmt.Printf("error: %v\n", err)

	_ = result
	_ = err

	dup.Close()
}*/
