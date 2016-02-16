package main

import (
	"bufio"
	"errors"
	"fmt"
	// "os"
	"os/exec"
	"time"
)

var check func(string, error) = func(what string, e error) {
	if e != nil {
		fmt.Println("Error", what+":", e.Error())
		/*os.Exit(1000)*/
		panic(e)
	}
}

func main() {
	cmd := exec.Command("go", "run", "loop.go")
	//cmd := exec.Command("cat")
	stdin, err := cmd.StdinPipe()
	check("stdin", err)
	defer stdin.Close()

	stdout, err := cmd.StdoutPipe()
	check("stdout", err)
	defer stdout.Close()

	err = cmd.Start()
	check("Start", err)
	fmt.Println("Starting")

	bufin := bufio.NewWriter(stdin)
	bufout := bufio.NewReader(stdout)

	for i := 1; i <= 10; i++ {
		fmt.Println("Attempt sending data ", i)
		if i == 1 {
			SendIn(bufin, "dir")
		} else if i == 2 {
			SendIn(bufin, "dir")
		} else if i == 3 {
			SendIn(bufin, "dir")
		} else {
			SendIn(bufin, fmt.Sprintf("Command-%d", i))
		}

		if i == 10 {
			SendIn(bufin, "exit")
		}

		time.Sleep(time.Second)

		out := GetOut(bufout)
		_ = out
	}

	/*for {
		out := GetOut(bufout)
		if out == "exit" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}*/

	err = cmd.Wait()
	check("wait", err)
	fmt.Println("Done")
}

func SendIn(bufin *bufio.Writer, data string) {
	iwrite, ewrite := bufin.WriteString(data + "\n")

	check("write", ewrite)
	if iwrite == 0 {
		check("write", errors.New("Writing only 0 byte"))
	} else {
		err := bufin.Flush()
		check("Flush", err)
	}
}

func GetOut(bufout *bufio.Reader) string {
	bread, eread := bufout.ReadString('\n')
	if eread != nil && eread.Error() == "EOF" {
		return "exit"
	}
	check("read", eread)
	fmt.Println("Read: ", bread)
	return bread
}
