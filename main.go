package main

import (
	. "github.com/frezadev/hdc/hive"
	// "os"
)

var fnHR FnHiveReceive
var h *Hive

func main() {
	h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
	q := "select * from sample_07 limit 5;"
	hSess, e := h.Exec(q, fnHR)
	_ = hSess
	_ = e
}
