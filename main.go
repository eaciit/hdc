package main

import (
	"fmt"
	// "github.com/eaciit/toolkit"
	. "github.com/frezadev/hdc/hive"
	// "reflect"
	// "os"
)

var fnHR FnHiveReceive
var h *Hive

type Sample7 struct {
	Code        string `tag_name:"code"`
	Description string `tag_name:"description"`
	Total_emp   string `tag_name:"total_emp"`
	Salary      string `tag_name:"salary"`
}

func main() {
	h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
	q := "select * from sample_07 limit 5;"
	result, e := h.Exec(q)
	fmt.Printf("error: \n%s\n", e)
	fmt.Printf("result: \n%s\n", result)

	for _, res := range result {
		tmp := Sample7{}
		h.ParseOutputX(res, &tmp)
		fmt.Println(tmp)
	}

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

	/*var x = Sample7{}
	var z interface{}
	z = x
	s := reflect.ValueOf(&z).Elem()
	typeOfT := s.Type()
	fmt.Println(reflect.ValueOf(&z).Interface())
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		tag := s.Type().Field(i).Tag
		fmt.Printf("%d: %s %s = %v | tag %s \n", i, typeOfT.Field(i).Name, f.Type(), f.Interface(), tag.Get("tag_name"))

	}*/

	/*h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
	h.Header = []string{"code", "description", "total_emp", "salary"}
	qTest := "00-0000,All Occupations,134354250,40690"
	var result = Sample7{}
	h.ParseOutputX(qTest, &result)
	fmt.Printf("result: %s\n", result)*/
}
