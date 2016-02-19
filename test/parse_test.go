package parse_test

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
	res = []string{"'00-0000'\t'All Occupations TSV'\t'134354250'\t'40690'\t'2014-Dec-05'"}
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

	resj = []string{",\"description\" : \"All Occupations INTERFACE\" , \"total_emp\" : 222, \"salary\" : 2222,\"Date\" : \"2012-05-23T18:25:43Z\" },{ \"code\" : \"00-2222\",\"description\" : \"All Occupations NEXT\" , \"total_emp\" : 222, \"salary\" : 2222,\"Date\" : \"2012-05-23T18:25:43Z\" }", "{ \"code\" : \"00-2222\",\"description\" : \"All Occupations Last\" , \"total_emp\" : 222, \"salary\" : 2222,\"Date\" : \"2012-05-23T18:25:43Z\" }"}
	var tmpx interface{}
	h.ParseOutput(resj, &tmpx)
	log.Println(tmpx)

}

// func TestConstructHeader(t *testing.T) {
// 	h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@", "")
// 	h.ConstructHeader("'sample_07.code'\t'sample_07.description'\t'sample_07.total_emp'\t'sample_07.salary'", "\t")
// 	log.Println(h.Header)
// }

// func TestUpperGoroutine(t *testing.T) {
// 	jobs := make(chan int, 1000)
// 	results := make(chan int, 1000)

// 	for w := 1; w <= 100; w++ {
// 		go worker(w, jobs, results)
// 	}

// 	for j := 1; j <= 150; j++ {
// 		jobs <- j
// 	}

// 	close(jobs)

// 	for a := 1; a <= 150; a++ {
// 		<-results
// 	}

// }

// func worker(id int, jobs <-chan int, results chan<- int) {
// 	for j := range jobs {
// 		log.Println("worker", id, "processing job", j)
// 		time.Sleep(time.Second)
// 		results <- j * 2
// 	}
// }
