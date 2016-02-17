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

func main() {
	// cmd := exec.Command("sh", "-c", "beeline --outputFormat=csv2 -u jdbc:hive2://192.168.0.223:10000/default -n developer -d org.apache.hive.jdbc.HiveDriver")

	cmd := exec.Command("sh", "-c", "ls")

	stdin, err := cmd.StdinPipe()
	check("stdin", err)
	defer stdin.Close()

	stdout, err := cmd.StdoutPipe()
	check("stdout", err)
	defer stdout.Close()

	bufin := bufio.NewWriter(stdin)
	bufout := bufio.NewReader(stdout)

	dup := DuplexTerm{}
	dup.Writer = bufin
	dup.Reader = bufout

	err = cmd.Start()
	check("Start", err)

	done := make(chan bool)

	go func() {
		for i := 1; i < 3; i++ {
			if i == 1 {
				result, err := dup.SendInput("ls")
				_ = result
				_ = err
			}
			if i == 2 {
				result, err := dup.SendInput("ls")
				_ = result
				_ = err
			}
		}
		done <- true
	}()
	//_ = result

	<-done

	cmd.Wait()
	//check("wait", err)
	fmt.Println("Done")
	/*stdin.Close()
	stdout.Close()*/
}
