package test

import (
	"fmt"
	"github.com/eaciit/toolkit"
	. "github.com/frezadev/hdc/hive"
	// . "github.com/eaciit/hdc/hive"
	//. "github.com/RyanCi/hdc/hive"
	"os"
	"testing"
)

var h *Hive
var e error

type Sample7 struct {
	Code        string `tag_name:"code"`
	Description string `tag_name:"description"`
	Total_emp   string `tag_name:"total_emp"`
	Salary      string `tag_name:"salary"`
}

type students struct {
	name    string
	age     int
	phone   string
	address string
}

func killApp(code int) {
	if h != nil {
		h.Conn.Close()
	}
	os.Exit(code)
}

func fatalCheck(t *testing.T, what string, e error) {
	if e != nil {
		t.Fatalf("%s: %s", what, e.Error())
	}
}

func TestHiveConnect(t *testing.T) {
	h = HiveConfig("192.168.0.223:10000", "default", "hdfs", "", "")
}

/* Populate will exec query and immidiately return the value into object
Populate is suitable for short type query that return limited data,
Exec is suitable for long type query that return massive amount of data and require time to produce it

Ideally Populate should call Exec as well but already have predefined function on it receiving process
*/
func TestHivePopulate(t *testing.T) {
	q := "select * from sample_07 limit 5;"

	var result []toolkit.M

	h.Conn.Open()

	e := h.Populate(q, &result)
	fatalCheck(t, "Populate", e)

	if len(result) != 5 {
		t.Logf("Error want %d got %d", 5, len(result))
	}

	t.Logf("Result: \n%s", toolkit.JsonString(result))

	h.Conn.Close()
}

func TestHiveExec(t *testing.T) {
	i := 0
	q := "select * from sample_07 limit 5;"

	h.Conn.Open()

	e := h.Exec(q, func(x HiveResult) error {
		i++
		t.Logf("Receiving data: %s", toolkit.JsonString(x))
		return nil
	})

	if e != nil {
		t.Fatalf("Error exec query: %s", e.Error())
	}

	if i < 5 {
		t.Fatalf("Error receive result. Expect %d got %d", 5, i)
	}

	h.Conn.Close()
}

func TestHiveExecMulti(t *testing.T) {
	h.Conn.Open()

	var ms1, ms2 []HiveResult
	q := "select * from sample_07 limit 5"

	e := h.Exec(q, func(x HiveResult) error {
		ms1 = append(ms1, x)
		return nil
	})

	fatalCheck(t, "HS1 exec", e)

	e = h.Exec(q, func(x HiveResult) error {
		ms2 = append(ms2, x)
		return nil
	})

	fatalCheck(t, "HS2 Exec", e)

	t.Logf("Value of HS1\n%s\n\nValue of HS2\n%s", toolkit.JsonString(ms1), toolkit.JsonString(ms2))

	h.Conn.Close()
}

func TestLoad(t *testing.T) {
	h.Conn.Open()

	var student students

	retVal, err := h.Load("students", "|", &student)

	if err != nil {
		t.Log(err)
	}
	h.Conn.Close()
	t.Log(retVal)
}

//for now, this function works on simple csv file
func TestLoadFile(t *testing.T) {
	h.Conn.Open()

	var student students

	retVal, err := h.LoadFile("/home/developer/contoh.txt", "students", "txt", &student)

	if err != nil {
		t.Log(err)
	}
	h.Conn.Close()
	t.Log(retVal)
}
