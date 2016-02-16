package main

import (
	"fmt"
	. "github.com/eaciit/hdc/hive"
)

var h *Hive
var q string

type Sample7 struct {
	Code        string `tag_name:"code"`
	Description string `tag_name:"description"`
	Total_emp   string `tag_name:"total_emp"`
	Salary      string `tag_name:"salary"`
}

func main() {
	h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@", "")
	q = "select * from sample_07 limit 20;"
	
	//for now this function just provide  csv type
	TestParseOutput()

	//Exec Query and Process with DoSomething Function PerLine | EXECPERLINE only support for csv or tsv
	h.OutputType = "csv"
	TestExecPerLine()
	
	// Exec Query and wait until all line fetched | EXEC only support for csv or tsv
	h.OutputType = "tsv"
	TestExec()
}

func DoSomething(res string) {
	tmp := Sample7{}
	h.ParseOutput(res, &tmp)
	fmt.Println(tmp)
}

func TestExec() {
	res, e := h.Exec(q)

	if e != nil {
		fmt.Printf("error: \n%v\n", e)
	} else {
		fmt.Println(res)
	}
}

func TestExecPerLine() {
	e := h.ExecLine(q, DoSomething)

	if e != nil {
		fmt.Printf("error: \n%v\n", e)
	}
}

func TestParseOutput() {
	h.Header = []string{"code", "description", "total_emp", "salary"}

	h.OutputType = "csv"
	res := "'00-0000','All Occupations CSV','134354250','40690'"
	tmp := Sample7{}
	h.ParseOutput(res, &tmp)
	fmt.Println(tmp)

	h.OutputType = "tsv"
	res = "00-0000\tAll Occupations TSV\t134354250\t40690"
	tmp = Sample7{}
	h.ParseOutput(res, &tmp)
	fmt.Println(tmp)

	h.OutputType = "json"
	res = "{ \"code\" : \"00-0000\" , \"description\" : \"All Occupations JSON\", \"total_emp\" : 134354250, \"salary\" : 40690}"
	tmp = Sample7{}
	h.ParseOutput(res, &tmp)
	fmt.Println(tmp)
}
