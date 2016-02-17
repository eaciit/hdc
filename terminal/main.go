package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

/*var check func(string, error) = func(what string, e error) {
	if e != nil {
		fmt.Println("Error", what+":", e.Error())
		// os.Exit(1000)
		panic(e)
	}
}*/

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

func (d *DuplexTerm) Open() (e error) {
	d.Cmd = exec.Command("sh", "-c", "beeline --outputFormat=csv2 -u jdbc:hive2://192.168.0.223:10000/default -n developer -d org.apache.hive.jdbc.HiveDriver")

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
	d.Cmd.Wait()
	d.Stdin.Close()
	d.Stdout.Close()
}

func main() {
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

	/*for {
		bread, eread := dup.Reader.ReadString('\n')
		if eread != nil && eread.Error() == "EOF" {
			break
		}
		fmt.Println(strings.TrimRight(bread, "\n"))
	}*/

	dup.Close()
}
