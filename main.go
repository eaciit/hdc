package main

import (
	"fmt"
	. "github.com/frezadev/hdc/hive"
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
	h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
	q := "select * from sample_07 limit 5;"
	result, e := h.Exec(q)
	fmt.Printf("error: \n%s\n", e)
	fmt.Printf("result: \n%s\n", result)

	//obj, e := h.ParseOutput(result, Sample7{})
	//_ = obj
}
