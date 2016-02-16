package hive

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/eaciit/cast"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"encoding/json"
	"os"
	"os/exec"
	"os/user"
	"reflect"
	"strconv"
	"strings"
)

const (
	BEE_TEMPLATE = "%sbeeline -u jdbc:hive2://%s/%s"
	BEE_USER     = " -n %s"
	BEE_PASSWORD = " -p %s"
	BEE_QUERY    = " -e \"%s\""
	/*SHOW_HEADER  = " --showHeader=true"
	HIDE_HEADER  = " --showHeader=false"*/
	CSV_FORMAT = " --outputFormat=csv"
	TSV_FORMAT = " --outputFormat=tsv"
	/*DSV_FORMAT    = " --outputFormat=dsv --delimiterForDSV=|\t"
	DSV_DELIMITER = "|\t"*/
)

// type FnHiveReceive func(string) (interface{}, error)

type Hive struct {
	BeePath     string
	Server      string
	User        string
	Password    string
	DBName      string
	HiveCommand string
	Header      []string
	OutputType 	string
}

func HiveConfig(server, dbName, userid, password, path string,delimiter ...string) *Hive {
	hv := Hive{}
	hv.BeePath = path
	hv.Server = server
	hv.Password = password

	if dbName == "" {
		dbName = "default"
	}

	hv.DBName = dbName

	if userid == "" {
		user, err := user.Current()
		if err == nil {
			userid = user.Username
		}
	}

	hv.OutputType = "tsv"
	if len(delimiter) > 0 && delimiter[0] == "csv" {
		hv.OutputType = "csv"
	}

	hv.User = userid

	return &hv
}

func SetHeader(header []string) *Hive {
	hv := Hive{}
	hv.Header = header
	return &hv
}

func (h *Hive) cmdStr(arg ...string) (out string) {
	out = fmt.Sprintf(BEE_TEMPLATE, h.BeePath, h.Server, h.DBName)

	if h.User != "" {
		out += fmt.Sprintf(BEE_USER, h.User)
	}

	if h.Password != "" {
		out += fmt.Sprintf(BEE_PASSWORD, h.Password)
	}

	for _, value := range arg {
		out += value
	}

	out += fmt.Sprintf(BEE_QUERY, h.HiveCommand)
	return
}

func (h *Hive) command(cmd ...string) *exec.Cmd {
	arg := append([]string{"-c"}, cmd...)
	return exec.Command("sh", arg...)
}

func (h *Hive) constructHeader(header string,delimiter string) {
	var tmpHeader []string
	for _, header := range strings.Split(header, delimiter) {
		split := strings.Split(header, ".")
		if len(split) > 1 {
			tmpHeader = append(tmpHeader, strings.Trim(split[1]," '"))
		} else {
			tmpHeader = append(tmpHeader, strings.Trim(header," '"))
		}
	}
	h.Header = tmpHeader
}

func (h *Hive) Exec(query string) (out []string, e error) {
	h.HiveCommand = query
	cmd := h.command()

	delimiter :="\t"
	if h.OutputType == "csv" {
		cmd = h.command(h.cmdStr(CSV_FORMAT))
		delimiter = ","
	}else{
		cmd = h.command(h.cmdStr(TSV_FORMAT))
	}

	outByte, e := cmd.Output()
	result := strings.Split(string(outByte), "\n")

	if len(result) > 0 {
		h.constructHeader(result[:1][0],delimiter)
	}

	//fmt.Printf("header: %v\n", h.Header)

	if len(result) > 1 {
		out = result[1:]
	}
	return
}

func (h *Hive) ExecLine(query string, DoResult func(result string)) (e error) {
	h.HiveCommand = query
	cmd := h.command()

	delimiter :="\t"
	if h.OutputType == "csv" {
		cmd = h.command(h.cmdStr(CSV_FORMAT))
		delimiter = ","
	}else{
		cmd = h.command(h.cmdStr(TSV_FORMAT))
	}

	cmdReader, e := cmd.StdoutPipe()

	if e != nil {
		fmt.Fprintln(os.Stderr, "Error creating stdoutPipe for cmd", e)
	}

	scanner := bufio.NewScanner(cmdReader)

	idx := 1

	go func(idx int) {
		for scanner.Scan() {
			resStr := scanner.Text()
			if idx == 1 {
				h.constructHeader(resStr,delimiter)
			} else {
				DoResult(resStr)
			}
			idx += 1
		}
	}(idx)

	e = cmd.Start()

	if e != nil {
		fmt.Fprintln(os.Stderr, "Error starting Cmd", e)
	}

	e = cmd.Wait()

	if e != nil {
		fmt.Fprintln(os.Stderr, "Error waiting Cmd", e)
	}

	return
}

func (h *Hive) ExecFile(filepath string) (e error) {
	file, e := os.Open(filepath)
	if e != nil {
		fmt.Println(e)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
		h.Exec(scanner.Text())
	}

	if e = scanner.Err(); e != nil {
		fmt.Println(e)
	}

	return
}

func (h *Hive) ExecNonQuery(query string) (e error) {
	cmd := exec.Command("sh", "-c", h.cmdStr())
	out, err := cmd.Output()
	if err == nil {
		fmt.Printf("result: %s\n", out)
	} else {
		fmt.Printf("result: %s\n", err)
	}
	return err
}

func (h *Hive) ImportHDFS(HDFSPath, TableName, Delimiter string, TableModel interface{}) (retVal string, err error) {
	retVal = "process failed"
	tempVal, err := h.Exec("select '1' from " + TableName + " limit 1")

	if tempVal == nil {
		tempQuery := ""

		var v reflect.Type
		v = reflect.TypeOf(TableModel).Elem()

		if v.Kind() == reflect.Struct {
			tempQuery = "create table " + TableName + " ("
			for i := 0; i < v.NumField(); i++ {
				if i == (v.NumField() - 1) {
					tempQuery += v.Field(i).Name + " " + v.Field(i).Type.String() + ") row format delimited fields terminated by '" + Delimiter + "'"
				} else {
					tempQuery += v.Field(i).Name + " " + v.Field(i).Type.String() + ", "
				}
			}
			tempVal, err = h.Exec(tempQuery)
		}
	}

	if err != nil {
		tempVal, err = h.Exec("load data local inpath '" + HDFSPath + "' overwrite into table " + TableName + ";")

		if err != nil {
			retVal = "success"
		}
	}

	return retVal, err

}

func (h *Hive) ParseOutput(in string, m interface{}) (e error) {

	if !toolkit.IsPointer(m) {
		return errorlib.Error("", "", "Fetch", "Model object should be pointer")
	}

	if h.OutputType == "csv"{
		var v reflect.Type
		v = reflect.TypeOf(m).Elem()
		ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

		appendData := toolkit.M{}
		iv := reflect.New(v).Interface()

			reader := csv.NewReader(strings.NewReader(strings.Replace(in,"'","\"",-1)))
			record, e := reader.Read()

			if e != nil {
				return e
			}

			if v.NumField() != len(record) {
				return &FieldMismatch{v.NumField(), len(record)}
			}

			for i, val := range h.Header {
				appendData[val] = strings.TrimSpace(record[i])
			}

			if v.Kind() == reflect.Struct {
				for i := 0; i < v.NumField(); i++ {
					tag := v.Field(i).Tag

					if appendData.Has(v.Field(i).Name) || appendData.Has(tag.Get("tag_name")) {
						switch v.Field(i).Type.Kind() {
						case reflect.Int:
							appendData.Set(v.Field(i).Name, cast.ToInt(appendData[v.Field(i).Name], cast.RoundingAuto))
						case reflect.Float32:
							valf, _ := strconv.ParseFloat(appendData[v.Field(i).Name].(string), 32)
							appendData.Set(v.Field(i).Name, valf)
						case reflect.Float64:
							valf,_ := strconv.ParseFloat(appendData[v.Field(i).Name].(string), 64)
							appendData.Set(v.Field(i).Name, valf)
						}
					}
				}
			}

			toolkit.Serde(appendData, iv, "json")
			ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())
			reflect.ValueOf(m).Elem().Set(ivs.Index(0))
		}else if h.OutputType == "json"{
			var temp = toolkit.M{}
			e := json.Unmarshal([]byte(in), temp)
			if e != nil {
				return e
			}
			e = toolkit.Serde(temp, m, "json")
			if e != nil {
				return e
			}
		}else{
				var v reflect.Type		
			 	v = reflect.TypeOf(m).Elem()		
			 	ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)		
			 		
			 	appendData := toolkit.M{}		
			 	iv := reflect.New(v).Interface()		
			 		
			 	splitted := strings.Split(in, "\t")		
			 		
			 	for i, val := range h.Header {		
			 		appendData[val] = strings.TrimSpace(strings.Trim(splitted[i], " '"))		
			 	}		
			 		
			 	if v.Kind() == reflect.Struct {		
			 		for i := 0; i < v.NumField(); i++ {		
			 		tag := v.Field(i).Tag

			 		if appendData.Has(v.Field(i).Name) || appendData.Has(tag.Get("tag_name")) {		
			 				switch v.Field(i).Type.Kind() {		
			 				case reflect.Int:		
			 					appendData.Set(v.Field(i).Name, cast.ToInt(appendData[v.Field(i).Name], cast.RoundingAuto))		
			 				case reflect.Float32:		
			 					valf, _ := strconv.ParseFloat(appendData[v.Field(i).Name].(string), 32)		
			 					appendData.Set(v.Field(i).Name, valf)		
			 				case reflect.Float64:		
			 					valf, _ := strconv.ParseFloat(appendData[v.Field(i).Name].(string), 64)		
			 					appendData.Set(v.Field(i).Name, valf)		
			 				}		
			 			}		
			 		}		
			 	}		
			 		
			 	toolkit.Serde(appendData, iv, "json")		
			 	ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())		
			 	reflect.ValueOf(m).Elem().Set(ivs.Index(0))		
			 	return nil
		}
	return nil
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
