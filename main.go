package main

import (
	"fmt"
	. "github.com/frezadev/hdc/hive"
	"reflect"
	// "os"
)

var fnHR FnHiveReceive
var h *Hive

type Sample7 struct {
	Code        string
	Description string
	Total_emp   string
	Salary      string
}

func main() {
	/*h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
	q := "select * from sample_07 limit 5;"
	result, e := h.Exec(q)
	fmt.Printf("error: \n%s\n", e)
	fmt.Printf("result: \n%s\n", result)*/

	//to execute query and read the result per line and then process its result
	/*var DoSomething = func(res interface{}) {
		fmt.Println(res)
	}

	resultline, e := h.ExecLine(q, DoSomething)
	fmt.Printf("error: \n%s\n", e)
	fmt.Printf("result: \n%s\n", resultline)*/

	/*obj, e := h.ParseOutput(nil, Sample7{})
	_ = e

	for _, value := range obj {
		fmt.Printf("obj: %v\n", value)
	}*/

	// test := "00-0000,All Occupations,134354250,40690"

	var x interface{}
	x = Sample7{}
	fmt.Println(reflect.TypeOf(x))
	fmt.Println(reflect.ValueOf(x))
}
