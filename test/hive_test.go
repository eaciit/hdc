package hive_test

import (
	. "github.com/hdc/yanda15/hdc/hive"
	//"os/exec"
	"testing"
	//"fmt"
	"os"

)

/*func TestHiveConnect(t *testing.T) {
	cmdStr := "beeline -u jdbc:hive2://192.168.0.223:10000/default -n developer -p b1gD@T@ -e \"select * from sample_07 limit 10;\""
	cmd := exec.Command("sh", "-c", cmdStr)
	out, err := cmd.Output()
	log.Printf("cmd: %s\n", cmd)
	log.Printf("out: %s\n", out)
	log.Printf("result: %v\n", err.Error())
}*/

//var e error
var fnHR FnHiveReceive

//var hSess HiveSession
var h *Hive

type Test struct{
	Name string
	NIK int
	Score float64
}

func TestParseOutPerLine(t *testing.T) {
	var xx = Test{} 
	err := ParseOutPerLine("|Yanda  |163  |6.5  |",[]string{"Name","NIK","Score"},"|",&xx)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	}
}

func killApp(code int) {
	os.Exit(code)
}

func TestHiveConnect(t *testing.T) {
	h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
}

func TestHiveExec(t *testing.T) {
	q := "select * from sample_07 limit 5;"
	hSess, e := h.Exec(q)
	_ = hSess
	_ = e
}

/*func TestHiveExecFile(t *testing.T) {
	path := "/home/developer/hive.txt"
	hSess, e := h.Exec(path, fnHR)
}*/
