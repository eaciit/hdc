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
	cmd := exec.Command("sh", "-c", "beeline --outputFormat=csv2 -u jdbc:hive2://192.168.0.223:10000/default -n developer -d org.apache.hive.jdbc.HiveDriver")
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

	SendIn(bufin, "select * from sample_07 limit 5;")
	time.Sleep(time.Second)
	out := GetOut(bufout)

	/*SendIn(bufin, "select * from sample_07 limit 5;")
	time.Sleep(time.Second)
	out = GetOut(bufout)

	SendIn(bufin, "select * from sample_07 limit 5;")
	time.Sleep(time.Second)
	out = GetOut(bufout)*/

	SendIn(bufin, "!quit;")

	err = cmd.Wait()
	check("wait", err)
	fmt.Println("Done")

	_ = out
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
	scanner := bufio.NewScanner(bufout)
	for scanner.Scan() {
		fmt.Println("Read: ", scanner.Text())
	}

	/*bread, eread := bufout.ReadString('\n')
	if eread != nil && eread.Error() == "EOF" {
		return "!quit;"
	}
	check("read", eread)
	// fmt.Println("Read: ", bread)
	return bread*/
	return ""
}
