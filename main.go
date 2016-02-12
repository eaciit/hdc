package main

import (
	"fmt"
	// "github.com/eaciit/toolkit"
	. "github.com/frezadev/hdc/hive"
	"reflect"
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

	var x = Sample7{}
	/*x = Sample7{
		Code:        "123",
		Description: "desc",
		Total_emp:   "5",
		Salary:      "1000",
	}*/

	/*s := reflect.ValueOf(&x).Elem()
	typeOf := s.Type()
	o := s.Interface()

	fmt.Println(o)

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		fmt.Printf("%d: %s %s = %v\n", i, typeOf.Field(i).Name, f.Type(), f.Interface())
	}*/

	s := reflect.ValueOf(&x).Elem()
	typeOfT := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		tag := s.Type().Field(i).Tag
		fmt.Printf("%d: %s %s = %v | tag %s \n", i, typeOfT.Field(i).Name, f.Type(), f.Interface(), tag.Get("tag_name"))

	}

}
