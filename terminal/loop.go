package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	cmd := ""
	buf := bufio.NewReader(os.Stdin)

	for cmd != "exit\n" {
		fmt.Print("Enter command: ")
		cmd, _ = buf.ReadString('\n')
		fmt.Println("You exec ", cmd)
	}
}
