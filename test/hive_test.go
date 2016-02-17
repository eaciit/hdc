package hive_test

import (
	. "github.com/hdc/hive"
	//"os/exec"
	"github.com/eaciit/toolkit"
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

type Test struct {
	Name  string
	NIK   int
	Score float64
}

func TestParseOutPerLine(t *testing.T) {
	var xx = Test{}
	err := ParseOutPerLine("|Yanda  |163  |6.5  |", []string{"Name", "NIK", "Score"}, "|", &xx)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	}
}

func killApp(code int) {
	if h != nil {
		h.Close()
	}
	os.Exit(code)
}

func TestHiveConnect(t *testing.T) {
	h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
}

func TestHiveExec(t *testing.T) {
	var ms []toolkit.M
	i := 0
	q := "select * from sample_07 limit 5;"
	// func exec(query string, model interface{}, receive interface{}=>func) (hivesession, error)

	hs, e := h.Exec(q, toolkit.M{}, func(x toolkit.M) error {
		i++
		t.Logf("Receiving data: %s", toolkit.JsonString(x))
	})

	if e != nil {
		t.Fatalf("Error exec query: %s", e.Error())
	}

	if i < 5 {
		t.Fatalf("Error receive result. Expect %d got %d", 5, i)
	}
}

func TestHiveClose(t *testing.T) {
	if h != nil {
		h.Close()
	}
}

/*func TestHiveExecFile(t *testing.T) {
	path := "/home/developer/hive.txt"
	hSess, e := h.Exec(path, fnHR)
}*/
