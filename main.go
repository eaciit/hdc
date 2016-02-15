package main

import (
	"fmt"
	// "github.com/eaciit/toolkit"
	//. "github.com/frezadev/hdc/hive"
	. "github.com/hdc/yanda15/hdc/hive"
	// "reflect"
	// "os"
)

var h *Hive

type Sample7 struct {
	Code        string `tag_name:"code"`
	Description string `tag_name:"description"`
	Total_emp   string `tag_name:"total_emp"`
	Salary      string `tag_name:"salary"`
}

func main() {
	// var e error
}

func DoSomething(res string) {
		tmp := Sample7{}
		h.ParseOutput(res, &tmp)
		fmt.Println(tmp)
}

func TestExec() {
	// var e error
	h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
	q := "select * from sample_07 limit 5;"
	res, e := h.Exec(q)

	if e !=nil{
		fmt.Printf("error: \n%v\n", e)
	}else{
		fmt.Println(res)
	}
}

func TestExecPerLine() {
	var e error
	h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
	q := "select * from sample_07 limit 5;"
	e = h.ExecLine(q, DoSomething)

	if e !=nil{
			fmt.Printf("error: \n%v\n", e)
	}
}

func TestParseOutput() {
	h = SetHeader([]string{"code","sample","description","total_emp","salary"})
	res := "00-0000,All Occupations,134354250,40690"
	tmp := Sample7{}
	h.ParseOutput(res, &tmp)
	fmt.Println(tmp)
}

