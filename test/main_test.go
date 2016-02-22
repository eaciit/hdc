package test

import (
	"fmt"
	"github.com/eaciit/toolkit"
	. "github.com/frezadev/hdc/hive"
	// . "github.com/eaciit/hdc/hive"
	//. "github.com/RyanCi/hdc/hive"
	// "reflect"
	"log"
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
		log.Printf("Error want %d got %d", 5, len(result))
	}

	log.Printf("Result: \n%s", toolkit.JsonString(result))

	h.Conn.Close()
}

func TestHiveExec(t *testing.T) {
	h.Conn.Open()

	var ms1, ms2 []HiveResult
	q := "select * from sample_07 limit 5"

	h.Conn.FnReceive = func(x HiveResult) error {
		ms1 = append(ms1, x)
		return nil
	}
	h.Exec(q)

	// fatalCheck(t, "HS1 exec", e)

	h.Conn.FnReceive = func(x HiveResult) error {
		ms2 = append(ms2, x)
		return nil
	}
	h.Exec(q)

	// fatalCheck(t, "HS2 Exec", e)

	t.Logf("Value of HS1\n%s\n\nValue of HS2\n%s", toolkit.JsonString(ms1), toolkit.JsonString(ms2))

	h.Conn.Close()
	/*q := "select * from sample_07 limit 1;"
	x := "select * from sample_07 limit 3;"

	DoSomething := func(res HiveResult) (e error) {
		toolkit.Serde(res, &res.ResultObj, "json")
		log.Printf("result: \n%v\n", res.ResultObj)
		return
	}

	DoElse := func(res HiveResult) (e error) {
		toolkit.Serde(res, &res.ResultObj, "json")
		log.Printf("limit 3: \n%v\n", res.ResultObj)
		return
	}

	h.Conn.Open()

	h.Conn.FnReceive = DoSomething
	h.Exec(q)

	h.Conn.FnReceive = DoElse
	h.Exec(x)

	h.Conn.Close()*/
}

func TestHiveExecMulti(t *testing.T) {
	var ms1 []HiveResult
	q := "select * from sample_07 limit 5;"

	DoSomething := func(res HiveResult) (e error) {
		ms1 = append(ms1, res)
		return
	}

	h.Conn.FnReceive = DoSomething
	h.Conn.Open()
	h.Exec(q)
	h.Exec(q)

	for _, v1 := range ms1 {
		log.Println(v1)
	}

	h.Conn.Close()
}

func TestLoad(t *testing.T) {
	h.Conn.Open()

	var student students

	retVal, err := h.Load("students", "|", &student)

	if err != nil {
		fmt.Println(err)
	}
	h.Conn.Close()
	fmt.Println(retVal)
}

//for now, this function works on simple csv file
func TestLoadFile(t *testing.T) {
	h.Conn.Open()

	var student students

	retVal, err := h.LoadFile("/home/developer/contoh.txt", "students", "txt", &student)

	if err != nil {
		fmt.Println(err)
	}
	h.Conn.Close()
	fmt.Println(retVal)
}
