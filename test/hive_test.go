package hive_test

import (
	"testing"
	"os/exec"
	"log"
	/*"fmt"
	"os"*/
)

func TestHiveConnect(t *testing.T) {
	cmdStr := "beeline -u jdbc:hive2://192.168.0.223:10000/default -n developer -p b1gD@T@ -e \"select * from sample_07 limit 10;\""
	cmd := exec.Command("sh", "-c", cmdStr)
	/*out, err := cmd.Output()
	log.Printf("cmd: %s\n", cmd)
	log.Printf("out: %s\n", out)
	log.Printf("result: %v\n", err.Error())*/

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	/*var person struct {
		Name string
		Age  int
	}
	if err := json.NewDecoder(stdout).Decode(&person); err != nil {
		log.Fatal(err)
	}*/
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}

	log.Printf("out: %s\n", stdout)
	//fmt.Printf("%s is %d years old\n", person.Name, person.Age)
}
