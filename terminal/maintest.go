package main

import (
	"fmt"
	"os/exec"
	"strings"
)

func main() {
	cmd := exec.Command("sh", "-c", "beeline")
	outByte, e := cmd.Output()
	result := strings.Split(string(outByte), "\n")
	fmt.Println(result)

	cmd = exec.Command("sh", "-c", "!connect", "!connect jdbc:hive2://192.168.0.223:10000/default developer org.apache.hive.jdbc.HiveDriver")
	outByte, e = cmd.Output()
	result = strings.Split(string(outByte), "\n")
	fmt.Println(result)

	cmd = exec.Command("sh", "-c", "select * from sample_07 limit 5")
	outByte, e = cmd.Output()
	result = strings.Split(string(outByte), "\n")
	fmt.Println(result)

	cmd = exec.Command("sh", "-c", "!quit")
	outByte, e = cmd.Output()
	result = strings.Split(string(outByte), "\n")
	fmt.Println(result)

	_ = e
}
