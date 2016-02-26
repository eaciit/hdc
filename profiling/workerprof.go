package main

import (
	. "github.com/frezadev/hdc/hive"
	// . "github.com/eaciit/hdc/hive"
	"github.com/pkg/profile"
	"log"
)

type Students struct {
	Name    string
	Age     int
	Phone   string
	Address string
}

func fatalCheck(what string, e error) {
	if e != nil {
		log.Fatalf("%s: %s", what, e.Error())
	}
}

func main() {
	defer profile.Start(profile.CPUProfile, profile.MemProfile, profile.BlockProfile).Stop()

	h := HiveConfig("192.168.0.223:10000", "default", "hdfs", "", "")
	err := h.Conn.Open()
	fatalCheck("Populate", err)

	var student Students

	totalWorker := 10
	retVal, err := h.LoadFileWithWorker("/home/developer/contoh.txt", "students", "csv", "dd/MM/yyyy", &student, totalWorker)

	if err != nil {
		fatalCheck("Populate", err)
	}

	h.Conn.Close()
	log.Printf("retVal: \n%v\n", retVal)
}
