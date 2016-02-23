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

func TestLoadFileWithWorker(t *testing.T) {
	h.Conn.Open()

	var student Students

	retVal, err := h.LoadFileWithWorker("/home/developer/contoh2.txt", "students", "txt", &student, 10)

	if err != nil {
		log.Println(err)
	}

	h.Conn.Close()
	log.Println(retVal)
}
