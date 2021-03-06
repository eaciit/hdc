package hive_test

import (
	. "github.com/eaciit/hdc/hive"
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

func fatalCheck(t *testing.T, what string, e error) {
	if e != nil {
		t.Fatalf("%s: %s", what, e.Error())
	}
}

func TestHiveExec(t *testing.T) {
	i := 0
	q := "select * from sample_07 limit 5;"
	// func exec(query string, model interface{}, receive interface{}=>func) (hivesession, error)

	hs, e := h.Exec(q, toolkit.M{}, func(x toolkit.M) error {
		i++
		t.Logf("Receiving data: %s", toolkit.JsonString(x))
		return nil
	})
	if e != nil {
		t.Fatalf("Error exec query: %s", e.Error())
	}

	e := hs.Wait()
	if e != nil {
		t.Fatalf("Error waiting for respond: %s", e.Error())
	}

	if i < 5 {
		t.Fatalf("Error receive result. Expect %d got %d", 5, i)
	}
}

func TestHivePopulate(t *testing.T) {
	var ms1 []toolkit.M
	q := "select * from sample_07 limit 5"

	/* Populate will exec query and immidiately return the value into object
	Populate is suitable for short type query that return limited data,
	Exec is suitable for long type query that return massive amount of data and require time to produce it

	Ideally Populate should call Exec as well but already have predefined function on it receiving process
	*/
	e := h.Populate(q, &ms1)
	fatalCheck(t, "Populate", e)

	if len(ms1) != 5 {
		t.Logf("Error want %d got %d", 5, len(ms1))
	}

	t.Logf("Result: %s", toolkit.JsonString(ms1))
}

func TestHiveExecMulti(t *testing.T) {
	var ms1, ms2 []toolkit.M
	q := "select * from sample_07 limit 5"

	hs1, e := h.Exec(q, toolkit.M{}, func(x toolkit.M) error {
		ms1 = append(ms1, x)
		return nil
	})
	fatalCheck(t, "HS1 exec", e)
	e = hs1.Wait()
	fatalCheck(t, "HS1 wait", e)

	hs2, e := h.Exec(q, toolkit.M{}, func(x toolkit.M) error {
		ms2 = append(ms2, x)
		return nil
	})
	fatalCheck(t, "HS2 exec", e)
	e = hs2.Wait()
	fatalCheck(t, "HS2 wait", e)

	for i, v1 := range ms1 {
		if i > len(ms2) {
			t.Fatalf("Len of both HS is not the same")
			return
		}
		v2 := ms2[i]
		for k, vm1 := range v1 {
			if !v2.Has(k) {
				t.Fatalf("Key not same")
			}
			vm2 := v2[k]
			if vm1 != vm2 {
				t.Fatalf("Value not the same")
			}
		}
	}

	t.Logf("Value of HS1\n%s\n\nValue of HS2\n%s", toolkit.JsonString(ms1), toolkit.JsonString(ms2))
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
