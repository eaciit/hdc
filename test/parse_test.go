package parse_test

import (
	. "github.com/eaciit/hdc/hive"
	"log"
	"testing"
	"time"
)

var h *Hive

type SampleParse struct {
	Code        string    `tag_name:"code"`
	Description string    `tag_name:"description"`
	Total_emp   int       `tag_name:"total_emp"`
	Salary      int       `tag_name:"salary"`
	Date        time.Time `tag_name:"date"`
}

func TestParseOutput(t *testing.T) {
	res := []string{"'00-0000','All Occupations CSV','134354250','40690','2014-05-01'", "'00-0000','All Occupations NEXT','134354250','40690','2014-05-01'"}
	tmp := []SampleParse{}
	e := Parse([]string{}, res, &tmp, "csv", "yyyy-MM-dd")
	log.Println(tmp)

	if e != nil {
		t.Error(e)
	}

	res = []string{"00-0000,All Occupations CSV2,134354250,40690,2014-Oct-01", "00-0000,All Occupations CSV2 NEXT,134354250,40690,2014-Dec-01"}
	tmp = []SampleParse{}
	e = Parse([]string{}, res, &tmp, "csv", "yyyy-MMM-dd")
	log.Println(tmp)

	if e != nil {
		t.Error(e)
	}

	res = []string{"'00-0000'\t'All Occupations TSV'\t'134354250'\t'40690'\t'2014-Dec-05'", "'00-0000'\t'All Occupations TSV NEXT'\t'134354250'\t'40690'\t'2014-Dec-05'"}
	tmp = []SampleParse{}
	e = Parse([]string{}, res, &tmp, "tsv", "yyyy-MMM-dd")
	log.Println(tmp)

	if e != nil {
		t.Error(e)
	}

	resj := []string{"{ \"code\" : \"00-0000\" , \"description\" : \"All Occupations JSON\" "}
	tmpj := []SampleParse{}
	e = Parse([]string{}, resj, &tmpj, "json", "")
	log.Println(tmpj)

	if e != nil {
		t.Error(e)
	}

	resj = []string{", \"total_emp\" : 134354, \"salary\" : 40690,\"Date\" : \"2012-04-23T18:25:43Z\" },{ \"code\" : \"00-2222\""}
	tmpj = []SampleParse{}
	e = Parse([]string{}, resj, &tmpj, "json", "")
	log.Println(tmpj)

	if e != nil {
		t.Error(e)
	}

	resj = []string{",\"description\" : \"All Occupations INTERFACE\" , \"total_emp\" : 222, \"salary\" : 2222,\"Date\" : \"2012-05-23T18:25:43Z\" },{ \"code\" : \"00-2222\",\"description\" : \"All Occupations NEXT\" , \"total_emp\" : 222, \"salary\" : 2222,\"Date\" : \"2012-05-23T18:25:43Z\" }", "{ \"code\" : \"00-2222\",\"description\" : \"All Occupations Last\" , \"total_emp\" : 222, \"salary\" : 2222,\"Date\" : \"2012-05-23T18:25:43Z\" }"}
	var tmpx interface{}
	e = Parse([]string{}, resj, &tmpx, "json", "")
	log.Println(tmpx)

	if e != nil {
		t.Error(e)
	}

}

func TestParseOutputOneStruct(t *testing.T) {

	//require fill header, because using interface as parameter
	res := "'00-0000','All Occupations CSV','134354250','40690','2014-05-01'"
	var tmp interface{}
	e := Parse([]string{"code", "desc", "emp", "sal", "date"}, res, &tmp, "csv", "yyyy-MM-dd")
	log.Println(tmp)

	if e != nil {
		t.Error(e)
	}

	res = "00-0000,All Occupations CSV2,134354250,40690,2014-05-01"
	tmpt := SampleParse{}
	e = Parse([]string{}, res, &tmpt, "csv", "yyyy-MM-dd")
	log.Println(tmpt)

	if e != nil {
		t.Error(e)
	}

	res = "'00-0000'\t'All Occupations TSV'\t'13.4354250'\t'40690'\t'2014-Dec-05'"
	var tmpz interface{}
	e = Parse([]string{"code", "desc", "emp", "sal", "date"}, res, &tmpz, "tsv", "yyyy-MMM-dd")
	log.Println(tmpz)

	if e != nil {
		t.Error(e)
	}

	//try to parse json with different line
	resj := "{ \"code\" : \"00-0000\" , \"description\" : \"All Occupations JSON\" "
	tmpj := SampleParse{}
	e = Parse([]string{}, resj, &tmpj, "json", "")
	log.Println(tmpj)

	if e != nil {
		t.Error(e)
	}

	resj = ", \"total_emp\" : 134354, \"salary\" : 40690,\"Date\" : \"2012-04-23T18:25:43Z\" },{ \"code\" : \"00-2222\""
	tmpj = SampleParse{}
	e = Parse([]string{}, resj, &tmpj, "json", "")
	log.Println(tmpj)

	if e != nil {
		t.Error(e)
	}

	resj = ",\"description\" : \"All Occupations INTERFACE\" , \"total_emp\" : 222, \"salary\" : 2222,\"Date\" : \"2012-05-23T18:25:43Z\" },{ \"code\" : \"00-2222\",\"description\" : \"All Occupations NEXT\" , \"total_emp\" : 222, \"salary\" : 2222,\"Date\" : \"2012-05-23T18:25:43Z\" }"
	var tmpx interface{}
	e = Parse([]string{}, resj, &tmpx, "json", "")
	log.Println(tmpx)

	if e != nil {
		t.Error(e)
	}

}
