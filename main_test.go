package hive

import (
	//"fmt"
	// "github.com/eaciit/toolkit"
	. "github.com/frezadev/hdc/hive"
	//. "github.com/eaciit/hdc/hive"
	// "reflect"
	// "os"
	"log"
	"testing"
)

var h *Hive

type Sample7 struct {
	Code        string `tag_name:"code"`
	Description string `tag_name:"description"`
	Total_emp   string `tag_name:"total_emp"`
	Salary      string `tag_name:"salary"`
}

func TestExec(t *testing.T) {
	var e error
	h = HiveConfig("192.168.0.223:10000", "default", "hdfs", "", "")
	q := "select * from sample_07 limit 5;"

	log.Println("---------------------- EXEC ----------------")

	h.Conn.Open()

	result, e := h.Exec(q)

	if e != nil {
		log.Printf("error: \n%v\n", e)

	} else {
		log.Printf("result: \n%v\n", result)

		for _, res := range result {
			tmp := Sample7{}
			h.ParseOutput(res, &tmp)
			log.Println(tmp)
		}
	}

	h.Conn.Close()
}

/*func TestExecLine(t *testing.T) {
	var e error
	h = HiveConfig("192.168.0.223:10000", "hdfs", "", "", "")
	q := "select * from sample_07 limit 5;"

	log.Println("---------------------- EXEC LINE ----------------")

	h.Conn.Open()

	// var DoSomething = func(res string) {
	// 	tmp := Sample7{}
	// 	h.ParseOutput(res, &tmp)
	// 	fmt.Println(tmp)
	// }

	// e = h.ExecLine(q, DoSomething)
	// fmt.Printf("error: \n%v\n", e)

	h.Conn.Close()
}*/

//func main() {

/*fmt.Println("---------------------- EXEC LINE ----------------")

//to execute query and read the result per line and then process its result

var DoSomething = func(res string) {
	tmp := Sample7{}
	h.ParseOutput(res, &tmp)
	fmt.Println(tmp)
}

e = h.ExecLine(q, DoSomething)
fmt.Printf("error: \n%v\n", e)*/

/*h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@", nil)
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
//}

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
