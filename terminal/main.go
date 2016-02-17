package main

import (
	"bufio"
	"errors"
	"fmt"
	// "os"
	"io"
	"os/exec"
	// "time"
	// "runtime"
	// "sync"
)

var check func(string, error) = func(what string, e error) {
	if e != nil {
		fmt.Println("Error", what+":", e.Error())
		/*os.Exit(1000)*/
		panic(e)
	}
}

type DuplexTerm struct {
	Writer *bufio.Writer
	Reader *bufio.Reader
	Cmd    *exec.Cmd
	Stdin  io.WriteCloser
	Stdout io.ReadCloser
}

func (d *DuplexTerm) SendInput(input string) (result string, e error) {
	iwrite, ewrite := d.Writer.WriteString(input + "\n")
	check("write", ewrite)
	if iwrite == 0 {
		check("write", errors.New("Writing only 0 byte"))
	} else {
		err := d.Writer.Flush()
		check("Flush", err)
	}

	for {
		bread, eread := d.Reader.ReadString('\n')
		if eread != nil && eread.Error() == "EOF" {
			break
		}
		check("read", eread)
		fmt.Println(bread)
	}

	return
}

func (d *DuplexTerm) Open() {
	var err error
	d.Cmd = exec.Command("sh", "-c", "beeline --outputFormat=csv2 -u jdbc:hive2://192.168.0.223:10000/default -n developer -d org.apache.hive.jdbc.HiveDriver")

	d.Stdin, err = d.Cmd.StdinPipe()
	check("stdin", err)

	d.Stdout, err = d.Cmd.StdoutPipe()
	check("stdout", err)

	d.Writer = bufio.NewWriter(d.Stdin)
	d.Reader = bufio.NewReader(d.Stdout)

	err = d.Cmd.Start()
	check("Start", err)
}

func (d *DuplexTerm) Close() {
	d.Cmd.Wait()
	d.Stdin.Close()
	d.Stdout.Close()
}

func main() {
	dup := DuplexTerm{}
	dup.Open()

	result, err := dup.SendInput("select * from sample_07 limit 5;")
	fmt.Printf("error: %v\n", err)
	result, err = dup.SendInput("!quit")
	fmt.Printf("error: %v\n", err)
	_ = result
	_ = err

	/*for {
		bread, eread := dup.Reader.ReadString('\n')
		if eread != nil && eread.Error() == "EOF" {
			break
		}
		check("read", eread)
		fmt.Println(bread)
	}*/

	defer dup.Close()
	fmt.Println("Done")
}
