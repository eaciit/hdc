package main

import (
	"fmt"
	. "github.com/hdc/yanda15/hdc/hive"
	// "os"
	"reflect"
	"github.com/eaciit/toolkit"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/cast"
	"strings"
	"strconv"
)

var fnHR FnHiveReceive
var h *Hive

type Test struct{
	Name string
	NIK int
	Score float64
}

func main() {
	// h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")
	// q := "select * from sample_07 limit 5;"
	// e := h.ExecPerline(q)
	// fmt.Printf("error: \n%s\n", e)
	// fmt.Printf("result: \n%s\n", result)
	var t = Test{} 
	err := ParseOutx("|Yanda  |163  |6.5  |",[]string{"Name","NIK","Score"},"|",&t)
	fmt.Println(t)
	// fmt.Println(t.Name)
	// fmt.Println(t.Address)
	if err != nil {
		fmt.Printf("Unable to fetch: %s \n", err.Error())
	}
}


func ParseOutx(stdout string,head []string,delim string, m interface{}) (e error) {

	if !toolkit.IsPointer(m) {
		return errorlib.Error("","","Fetch", "Model object should be pointer")
	}

	var v reflect.Type
	v = reflect.TypeOf(m).Elem()
	ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

	appendData := toolkit.M{}
	iv := reflect.New(v).Interface()

	splitted:= strings.Split(strings.Trim(stdout," "+delim),delim)

	for i, val := range head{
		appendData[val] = strings.TrimSpace(splitted[i])
	}

	if v.Kind() == reflect.Struct {
			for i := 0; i < v.NumField(); i++ {
				if appendData.Has(v.Field(i).Name) {
					switch v.Field(i).Type.Kind() {
					case reflect.Int:
						appendData.Set(v.Field(i).Name, cast.ToInt(appendData[v.Field(i).Name], cast.RoundingAuto))
					case reflect.Float64:
						valf,_ := strconv.ParseFloat(appendData[v.Field(i).Name].(string),64)
						appendData.Set(v.Field(i).Name, valf )
					}
				}
			}
	}


	toolkit.Serde(appendData, iv, "json")
	ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())
	reflect.ValueOf(m).Elem().Set(ivs.Index(0))
	return nil
}
