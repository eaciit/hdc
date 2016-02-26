package test

import (
	. "github.com/eaciit/hdc/hive"
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

type Students struct {
	Name    string
	Age     int
	Phone   string
	Address string
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

func TestLoadFile(t *testing.T) {
	err := h.Conn.Open()
	fatalCheck(t, "Populate", e)

	var Student Students
	//test csv
	retVal, err := h.LoadFile("/home/developer/contoh.txt", "students", "csv", "dd/MM/yyyy", &Student)

	var SportMatch SportMatch

	//test json
	retValSport, err := h.LoadFile("/home/developer/test json.txt", "SportMatch", "json", "dd/MM/yyyy", &SportMatch)

	if err != nil {
		t.Log(err)
	}
	h.Conn.Close()
	t.Log(retVal)
	t.Log(retValSport)
}

func TestLoadFileWithWorker(t *testing.T) {
	// err := h.Conn.Open()
	fatalCheck(t, "Populate", e)

	var Student Students
	//test csv
	retVal, err := h.LoadFile("/home/developer/contoh2.txt", "studentworker", "csv", "dd/MM/yyyy", &Student)

	var SportMatch SportMatch

	//test json
	retValSport, err := h.LoadFile("/home/developer/test json.txt", "sportmatchworker", "json", "dd/MM/yyyy", &SportMatch)

	if err != nil {
		t.Log(err)
	}
	// h.Conn.Close()
	t.Log(retVal)
	t.Log(retValSport)
}
