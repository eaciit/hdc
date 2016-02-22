package hive

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/eaciit/cast"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type HiveResult struct {
	Header     []string
	Result     []string
	ResultObj  interface{}
	DateFormat string
}

var JsonPart string

func (hr *HiveResult) constructHeader(header string, delimiter string) {
	var tmpHeader []string
	for _, header := range strings.Split(header, delimiter) {
		split := strings.Split(header, ".")
		if len(split) > 1 {
			tmpHeader = append(tmpHeader, strings.Trim(split[1], " '"))
		} else {
			tmpHeader = append(tmpHeader, strings.Trim(header, " '"))
		}
	}
	hr.Header = tmpHeader
}

func Parse(header []string, in interface{}, m interface{}, outputType string, dateFormat string) (e error) {
	if !toolkit.IsPointer(m) {
		return errorlib.Error("", "", "Fetch", "Model object should be pointer")
	}
	slice := false
	var ins []string
	if reflect.ValueOf(m).Elem().Kind() == reflect.Slice || toolkit.TypeName(in) == "[]string" {
		slice = true
		ins = in.([]string)
	} else {
		ins = append(ins, in.(string))
	}

	if outputType == CSV {
		var v reflect.Type

		if slice && toolkit.TypeName(m) != "*interface {}" {
			v = reflect.TypeOf(m).Elem().Elem()
		} else {
			v = reflect.TypeOf(m).Elem()
		}

		ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)
		for _, data := range ins {
			appendData := toolkit.M{}
			iv := reflect.New(v).Interface()
			reader := csv.NewReader(strings.NewReader(""))
			if strings.Contains(data, "','") {
				reader = csv.NewReader(strings.NewReader("\"" + strings.Trim(strings.Replace(data, "','", "\",\"", -1), "'") + "\""))
			} else {
				reader = csv.NewReader(strings.NewReader(data))
			}
			record, e := reader.Read()

			if e != nil {
				return e
			}

			if v.Kind() == reflect.Struct {
				for i := 0; i < v.NumField(); i++ {
					appendData[v.Field(i).Name] = strings.TrimSpace(record[i])
					valthis := appendData[v.Field(i).Name]
					switch v.Field(i).Type.Kind() {
					case reflect.Int:
						appendData.Set(v.Field(i).Name, cast.ToInt(valthis, cast.RoundingAuto))
					case reflect.Int16:
						appendData.Set(v.Field(i).Name, cast.ToInt(valthis, cast.RoundingAuto))
					case reflect.Int32:
						appendData.Set(v.Field(i).Name, cast.ToInt(valthis, cast.RoundingAuto))
					case reflect.Int64:
						appendData.Set(v.Field(i).Name, cast.ToInt(valthis, cast.RoundingAuto))
					case reflect.Float32:
						valf, _ := strconv.ParseFloat(valthis.(string), 32)
						appendData.Set(v.Field(i).Name, valf)
					case reflect.Float64:
						valf, _ := strconv.ParseFloat(valthis.(string), 64)
						appendData.Set(v.Field(i).Name, valf)
					case reflect.Bool:
						valf, _ := strconv.ParseBool(valthis.(string))
						appendData.Set(v.Field(i).Name, valf)
					default:
						dtype := DetectDataType(valthis.(string), dateFormat)
						if dtype == "date" {
							valf := cast.String2Date(valthis.(string), dateFormat)
							appendData.Set(v.Field(i).Name, valf)
						}
					}

				}

			} else {
				if len(header) == 0 {
					e = errorlib.Error("", "", "Parse Out", "Header cant be null because object is not struct")
					return e
				}

				for i, val := range header {
					appendData[val] = strings.TrimSpace(record[i])
					valthis := appendData[val]
					dtype := DetectDataType(valthis.(string), dateFormat)
					if dtype == "int" {
						appendData.Set(val, cast.ToInt(valthis, cast.RoundingAuto))
					} else if dtype == "float" {
						valf, _ := strconv.ParseFloat(valthis.(string), 64)
						appendData.Set(val, valf)
					} else if dtype == "date" {
						valf := cast.String2Date(valthis.(string), dateFormat)
						appendData.Set(val, valf)
					} else if dtype == "bool" {
						valf, _ := strconv.ParseBool(valthis.(string))
						appendData.Set(val, valf)
					}
				}
			}

			toolkit.Serde(appendData, iv, JSON)
			ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())
		}
		if slice {
			reflect.ValueOf(m).Elem().Set(ivs)
		} else {
			reflect.ValueOf(m).Elem().Set(ivs.Index(0))
		}
	} else if outputType == JSON {
		var temp interface{}
		ins = InspectJson(ins)

		//for catch multi json in one line
		if JsonPart != "" && slice {
			for {
				tempjsonpart := JsonPart
				JsonPart = ""
				tempIn := InspectJson([]string{tempjsonpart})
				if len(tempIn) == 0 {
					break
				} else {
					for _, tin := range tempIn {
						ins = append(ins, tin)
					}
				}
			}
		}

		forSerde := strings.Join(ins, ",")
		if slice {
			forSerde = fmt.Sprintf("[%s]", strings.Join(ins, ","))
		}
		if len(ins) > 0 {
			e := json.Unmarshal([]byte(forSerde), &temp)
			if e != nil {
				return e
			}
			e = toolkit.Serde(temp, m, "json")
			if e != nil {
				return e
			}
		}
	} else {
		var v reflect.Type

		if slice && toolkit.TypeName(m) != "*interface {}" {
			v = reflect.TypeOf(m).Elem().Elem()
		} else {
			v = reflect.TypeOf(m).Elem()
		}

		ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

		for _, data := range ins {
			appendData := toolkit.M{}
			iv := reflect.New(v).Interface()

			splitted := strings.Split(data, "\t")

			if v.Kind() == reflect.Struct {
				for i := 0; i < v.NumField(); i++ {
					appendData[v.Field(i).Name] = strings.TrimSpace(strings.Trim(splitted[i], " '"))
					valthis := appendData[v.Field(i).Name]
					switch v.Field(i).Type.Kind() {
					case reflect.Int:
						appendData.Set(v.Field(i).Name, cast.ToInt(valthis, cast.RoundingAuto))
					case reflect.Int16:
						appendData.Set(v.Field(i).Name, cast.ToInt(valthis, cast.RoundingAuto))
					case reflect.Int32:
						appendData.Set(v.Field(i).Name, cast.ToInt(valthis, cast.RoundingAuto))
					case reflect.Int64:
						appendData.Set(v.Field(i).Name, cast.ToInt(valthis, cast.RoundingAuto))
					case reflect.Float32:
						valf, _ := strconv.ParseFloat(valthis.(string), 32)
						appendData.Set(v.Field(i).Name, valf)
					case reflect.Float64:
						valf, _ := strconv.ParseFloat(valthis.(string), 64)
						appendData.Set(v.Field(i).Name, valf)
					case reflect.Bool:
						valf, _ := strconv.ParseBool(valthis.(string))
						appendData.Set(v.Field(i).Name, valf)
					default:
						dtype := DetectDataType(valthis.(string), dateFormat)
						if dtype == "date" {
							valf := cast.String2Date(valthis.(string), dateFormat)
							appendData.Set(v.Field(i).Name, valf)
						}
					}

				}

			} else {

				if len(header) == 0 {
					e = errorlib.Error("", "", "Parse Out", "Header cant be null because object is not struct")
					return e
				}

				for i, val := range header {
					appendData[val] = strings.TrimSpace(strings.Trim(splitted[i], " '"))
					valthis := appendData[val]
					dtype := DetectDataType(valthis.(string), dateFormat)
					if dtype == "int" {
						appendData.Set(val, cast.ToInt(valthis, cast.RoundingAuto))
					} else if dtype == "float" {
						valf, _ := strconv.ParseFloat(valthis.(string), 64)
						appendData.Set(val, valf)
					} else if dtype == "date" {
						valf := cast.String2Date(valthis.(string), dateFormat)
						appendData.Set(val, valf)
					} else if dtype == "bool" {
						valf, _ := strconv.ParseBool(valthis.(string))
						appendData.Set(val, valf)
					}
				}
			}

			toolkit.Serde(appendData, iv, JSON)
			ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())
		}

		if slice {
			reflect.ValueOf(m).Elem().Set(ivs)
		} else {
			reflect.ValueOf(m).Elem().Set(ivs.Index(0))
		}
	}
	return nil
}

func InspectJson(ins []string) (out []string) {
	var re []string

	for _, in := range ins {
		if JsonPart != "" {
			in = JsonPart + in
		}
		in = strings.Trim(strings.TrimSpace(in), " ,")
		charopen := 0
		charclose := 0
		for i, r := range in {
			c := string(r)
			if c == "{" {
				charopen += 1
			} else if c == "}" {
				charclose += 1
			}

			if charopen == charclose && (charclose != 0 && charopen != 0) {
				if len(in) == i+1 {
					JsonPart = ""
				} else {
					JsonPart = in[i+1:]
				}
				re = append(re, strings.Trim(strings.TrimSpace(in[:i+1]), " ,"))
				break
			}
			if charopen != charclose || (charclose == 0 && charopen == 0) {
				JsonPart = in
			}
		}

	}
	return re
}

func DetectDataType(in string, dateFormat string) (res string) {
	if in != "" {
		matchNumber := false
		matchFloat := false
		matchDate := false

		formatDate := "((^(0[0-9]|[0-9]|(1|2)[0-9]|3[0-1])(\\.|\\/|-)(0[0-9]|[0-9]|1[0-2])(\\.|\\/|-)[\\d]{4}$)|(^[\\d]{4}(\\.|\\/|-)(0[0-9]|[0-9]|1[0-2])(\\.|\\/|-)(0[0-9]|[0-9]|(1|2)[0-9]|3[0-1])$))"
		matchDate, _ = regexp.MatchString(formatDate, in)

		if !matchDate && dateFormat != "" {
			d := cast.String2Date(in, dateFormat)
			if d.Year() > 1 {
				matchDate = true
			}
		}

		x := strings.Index(in, ".")

		if x > 0 {
			matchFloat = true
			in = strings.Replace(in, ".", "", 1)
		}

		matchNumber, _ = regexp.MatchString("^\\d+$", in)

		if strings.TrimSpace(in) == "true" || strings.TrimSpace(in) == "false" {
			res = "bool"
		} else {
			res = "string"
			if matchNumber {
				res = "int"
				if matchFloat {
					res = "float"
				}
			}

			if matchDate {
				res = "date"
			}
		}
	}
	return res
}

type FieldMismatch struct {
	expected, found int
}

func (e *FieldMismatch) Error() string {
	return "CSV line fields mismatch. Expected " + strconv.Itoa(e.expected) + " found " + strconv.Itoa(e.found)
}

type UnsupportedType struct {
	Type string
}

func (e *UnsupportedType) Error() string {
	return "Unsupported type: " + e.Type
}
