package main

import (
	"fmt"
	. "github.com/eaciit/hdc/hive"
	"time"
)

var h *Hive
var q string

type Sample7 struct {
	Code        string `tag_name:"code"`
	Description string `tag_name:"description"`
	Total_emp   int    `tag_name:"total_emp"`
	Salary      int    `tag_name:"salary"`
}

type SampleParse struct {
	Code        string    `tag_name:"code"`
	Description string    `tag_name:"description"`
	Total_emp   int       `tag_name:"total_emp"`
	Salary      int       `tag_name:"salary"`
	Date        time.Time `tag_name:"date"`
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
	h.Header = []string{"code", "description", "total_emp", "salary", "Date"}

	h.OutputType = "csv"
	h.DateFormat = "YYYY-MM-DD"
	res := "'00-0000','All Occupations CSV','134354250','40690','2014-05-01'"
	tmp := SampleParse{}
	h.ParseOutput(res, &tmp)
	fmt.Println(tmp)

	h.OutputType = "csv2"
	h.DateFormat = "YYYY-MM-DD"
	res = "00-0000,All Occupations CSV2,134354250,40690,2014-05-01"
	tmp = SampleParse{}
	h.ParseOutput(res, &tmp)
	fmt.Println(tmp)

	h.OutputType = "tsv"
	h.DateFormat = "YYYY-MMM-DD"
	res = "00-0000\tAll Occupations TSV\t134354250\t40690\t2014-Dec-05"
	tmp = SampleParse{}
	h.ParseOutput(res, &tmp)
	fmt.Println(tmp)

	//try to parse json with different line
	h.OutputType = "json"
	res = "{ \"code\" : \"00-0000\" , \"description\" : \"All Occupations JSON\" "
	tmp = SampleParse{}
	h.ParseOutput(res, &tmp)
	fmt.Println(tmp)

	res = ", \"total_emp\" : 134354, \"salary\" : 40690,\"Date\" : \"2012-04-23T18:25:43Z\" },{ \"code\" : \"00-2222\""
	tmp = SampleParse{}
	h.ParseOutput(res, &tmp)
	fmt.Println(tmp)

	res = ",\"description\" : \"All Occupations INTERFACE\" , \"total_emp\" : 222, \"salary\" : 2222,\"Date\" : \"2012-05-23T18:25:43Z\" }"
	var tmpx interface{}
	h.ParseOutput(res, &tmpx)
	fmt.Println(tmpx)

}
