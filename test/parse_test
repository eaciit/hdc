package hive_test

import (
	. "github.com/yanda15/hdc/hive"
	"log"
	"testing"
	"time"
)

var h *Hive

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

func TestParseOutput(t *testing.T) {
	h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@", "")
	h.Header = []string{"Code", "Description", "Total_emp", "Salary", "Date"}

	h.OutputType = "csv"
	h.DateFormat = "yyyy-MM-dd"
	res := []string{"'00-0000','All Occupations CSV','134354250','40690','2014-05-01'"}
	tmp := []SampleParse{}
	h.ParseOutput(res, &tmp)
	log.Println(tmp)

	h.OutputType = "csv"
	h.DateFormat = "YYYY-MM-dd"
	res = []string{"00-0000,All Occupations CSV2,134354250,40690,2014-05-01"}
	tmp = []SampleParse{}
	h.ParseOutput(res, &tmp)
	log.Println(tmp)

	h.OutputType = "tsv"
	h.DateFormat = "YYYY-MMM-dd"
	res = []string{"00-0000\tAll Occupations TSV\t134354250\t40690\t2014-Dec-05"}
	tmp = []SampleParse{}
	h.ParseOutput(res, &tmp)
	log.Println(tmp)

	//try to parse json with different line
	h.OutputType = "json"
	resj := []string{"{ \"code\" : \"00-0000\" , \"description\" : \"All Occupations JSON\" "}
	tmpj := []SampleParse{}
	h.ParseOutput(resj, &tmpj)
	log.Println(tmpj)

	resj = []string{", \"total_emp\" : 134354, \"salary\" : 40690,\"Date\" : \"2012-04-23T18:25:43Z\" },{ \"code\" : \"00-2222\""}
	tmpj = []SampleParse{}
	h.ParseOutput(resj, &tmpj)
	log.Println(tmpj)

	resj = []string{",\"description\" : \"All Occupations INTERFACE\" , \"total_emp\" : 222, \"salary\" : 2222,\"Date\" : \"2012-05-23T18:25:43Z\" }"}
	var tmpx interface{}
	h.ParseOutput(resj, &tmpx)
	log.Println(tmpx)

}
