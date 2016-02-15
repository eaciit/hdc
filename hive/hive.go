package hive

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/eaciit/cast"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"os"
	"os/exec"
	"os/user"
	"reflect"
	"strconv"
	"strings"
)

const (
	BEE_TEMPLATE = "beeline -u jdbc:hive2://%s/%s -n %s -p %s"
	BEE_QUERY    = " -e \"%s\""
	/*SHOW_HEADER  = " --showHeader=true"
	HIDE_HEADER  = " --showHeader=false"*/
	CSV_FORMAT = " --outputFormat=csv2"
)

// type FnHiveReceive func(string) (interface{}, error)

type Hive struct {
	Server      string
	User        string
	Password    string
	DBName      string
	HiveCommand string
	Header      []string
}

func HiveConfig(server, dbName, userid, password string) *Hive {
	hv := Hive{}
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

	hv.User = userid

	return &hv
}

func ParseOut(s string) {
	fmt.Println(s)
}

func (h *Hive) cmdStr(arg ...string) (out string) {
	out = fmt.Sprintf(BEE_TEMPLATE, h.Server, h.DBName, h.User, h.Password)

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

func (h *Hive) constructHeader(header string) {
	var tmpHeader []string
	for _, header := range strings.Split(header, ",") {
		split := strings.Split(header, ".")
		if len(split) > 1 {
			tmpHeader = append(tmpHeader, split[1])
		} else {
			tmpHeader = append(tmpHeader, header)
		}
	}
	h.Header = tmpHeader
}

func (h *Hive) Exec(query string) (out []string, e error) {
	h.HiveCommand = query
	//fmt.Println(h.cmdStr(HIDE_HEADER, CSV_FORMAT))
	cmd := h.command(h.cmdStr(CSV_FORMAT))
	outByte, e := cmd.Output()
	result := strings.Split(string(outByte), "\n")

	if len(result) > 0 {
		h.constructHeader(result[:1][0])
	}

	fmt.Printf("header: %v\n", h.Header)

	if len(result) > 1 {
		out = result[1:]
	}
	return
}

func (h *Hive) ExecLine(query string, DoResult func(result string)) (e error) {
	h.HiveCommand = query
	cmd := h.command(h.cmdStr(CSV_FORMAT))
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
				h.constructHeader(resStr)
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

	var v reflect.Type
	v = reflect.TypeOf(m).Elem()
	ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

	appendData := toolkit.M{}
	iv := reflect.New(v).Interface()

	reader := csv.NewReader(strings.NewReader(in))
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
				case reflect.Float64:
					valf := cast.ToF64(appendData[v.Field(i).Name].(string), 2, cast.RoundingAuto) //strconv.ParseFloat(appendData[v.Field(i).Name].(string), 64)
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
