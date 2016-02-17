package main

import (
	"bufio"
	"errors"
	"fmt"
	// "os"
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

	/*for {
		bread, eread := d.Reader.ReadString('\n')
		if eread != nil && eread.Error() == "EOF" {
			break
		}
		check("read", eread)
		fmt.Println(bread)
	}*/

	return
}

func (d *DuplexTerm) Open() {
	d.Cmd = exec.Command("sh", "-c", "beeline --outputFormat=csv2 -u jdbc:hive2://192.168.0.223:10000/default -n developer -d org.apache.hive.jdbc.HiveDriver")

	stdin, err := d.Cmd.StdinPipe()
	check("stdin", err)
	//defer stdin.Close()

	stdout, err := d.Cmd.StdoutPipe()
	check("stdout", err)
	//defer stdout.Close()

	bufin := bufio.NewWriter(stdin)
	bufout := bufio.NewReader(stdout)

	d.Writer = bufin
	d.Reader = bufout

	err = d.Cmd.Start()
	check("Start", err)
}

func (d *DuplexTerm) Close() {
	d.Cmd.Wait()
}

func main() {
	dup := DuplexTerm{}
	dup.Open()

	done := make(chan bool)

	go func() {
		result, err := dup.SendInput("select * from sample_07 limit 5;")
		fmt.Printf("error: %v\n", err)
		result, err = dup.SendInput("!quit")
		fmt.Printf("error: %v\n", err)
		_ = result
		_ = err
		done <- true
	}()

	<-done

	for {
		bread, eread := dup.Reader.ReadString('\n')
		if eread != nil && eread.Error() == "EOF" {
			break
		}
		check("read", eread)
		fmt.Println(bread)
	}

	dup.Close()

	fmt.Println("Done")
}
