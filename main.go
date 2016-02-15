package main

import (
	"fmt"
	// "github.com/eaciit/toolkit"
	//. "github.com/frezadev/hdc/hive"
	. "github.com/eaciit/hdc/hive"
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
	var e error
	h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
	q := "select * from sample_07 limit 5;"

	/*fmt.Println("---------------------- EXEC ----------------")
	result, e := h.Exec(q)

	fmt.Printf("error: \n%v\n", e)

	for _, res := range result {
		tmp := Sample7{}
		//fmt.Println(res)
		h.ParseOutput(res, &tmp)
		fmt.Println(tmp)
	}*/

	fmt.Println("---------------------- EXEC LINE ----------------")

	//to execute query and read the result per line and then process its result

	var DoSomething = func(res string) {
		tmp := Sample7{}
		h.ParseOutput(res, &tmp)
		fmt.Println(tmp)
	}

	e = h.ExecLine(q, DoSomething)
	fmt.Printf("error: \n%v\n", e)

	/*h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
	h.Header = []string{"code", "description", "total_emp", "salary"}
	// qTest := "00-0000	All Occupations, asdfa,a dadsfasd	134354250	40690"
	//qTest := "00-0000 All Occupations 134354250       40690"
	qTest := "00-0000,All Occupations asdfa a dadsfasd,134354250,40690"
	var result = Sample7{}
	h.ParseOutput(qTest, &result)
	fmt.Printf("result: %s\n", result.Code)
	fmt.Printf("result: %s\n", result.Description)
	fmt.Printf("result: %s\n", result.Total_emp)
	fmt.Printf("result: %s\n", result.Salary)*/
}

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
