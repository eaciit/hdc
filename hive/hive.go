package hive

import (
	"bufio"
	"bytes"
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
	"time"
)

type FnHiveReceive func(string) (interface{}, error)

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

/*func (h *Hive) Connect() error {
	cmdStr := "beeline -u jdbc:hive2://" + h.Server + "/" + h.DBName + " -n " + h.User + " -p " + h.Password
	cmd := exec.Command("sh", "-c", cmdStr)
	out, err := cmd.Output()
	_ = out
	_ = err
	return nil
}*/

const (
	BEE_TEMPLATE = "beeline -u jdbc:hive2://%s/%s -n %s -p %s"
	BEE_QUERY    = " -e \"%s\""
	/*SHOW_HEADER  = " --showHeader=true"
	HIDE_HEADER  = " --showHeader=false"*/
	CSV_FORMAT = " --outputFormat=csv2"
)

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

func (h *Hive) ExecPerline(query string) (e error) {
	h.HiveCommand = query
	cmd := h.command(h.cmdStr())
	randomBytes := &bytes.Buffer{}
	cmd.Stdout = randomBytes
	err := cmd.Start()

	if err != nil {
		return err
	}

	outlength := 0
	ticker := time.NewTicker(time.Millisecond)
	go func(ticker *time.Ticker) {
		for range ticker.C {
			lenlength := len(strings.Split(strings.TrimSpace(randomBytes.String()), "\n"))
			if outlength < lenlength {
				for {
					if strings.Split(strings.TrimSpace(randomBytes.String()), "\n")[outlength] != "" {
						str := strings.Split(strings.TrimSpace(randomBytes.String()), "\n")[outlength]
						ParseOut(str)
						outlength += 1
						if outlength == lenlength {
							break
						}
					}
				}
			}
		}
	}(ticker)

	cmd.Wait()
	time.Sleep(time.Second * 2)

	return nil
}

func (h *Hive) ExecLine(query string, DoResult func(result interface{})) (e error) {
	h.HiveCommand = query
	cmd := h.command(h.cmdStr(CSV_FORMAT))
	cmdReader, e := cmd.StdoutPipe()

	if e != nil {
		fmt.Fprintln(os.Stderr, "Error creating stdoutPipe for cmd", e)
	}

	scanner := bufio.NewScanner(cmdReader)

	go func() {
		for scanner.Scan() {
			DoResult(scanner.Text())
			//fmt.Printf("out | %s\n", scanner.Text())
		}
	}()

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

func (h *Hive) ExecFile(filepath string) (hs *HiveSession, e error) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
		h.Exec(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	return nil, nil
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

func ParseOutPerLine(stdout string, head []string, delim string, m interface{}) (e error) {

	if !toolkit.IsPointer(m) {
		return errorlib.Error("", "", "Fetch", "Model object should be pointer")
	}

	var v reflect.Type
	v = reflect.TypeOf(m).Elem()
	ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

	appendData := toolkit.M{}
	iv := reflect.New(v).Interface()

	splitted := strings.Split(strings.Trim(stdout, " "+delim), delim)

	for i, val := range head {
		appendData[val] = strings.TrimSpace(splitted[i])
	}

	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			if appendData.Has(v.Field(i).Name) {
				switch v.Field(i).Type.Kind() {
				case reflect.Int:
					appendData.Set(v.Field(i).Name, cast.ToInt(appendData[v.Field(i).Name], cast.RoundingAuto))
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

/*func (h *Hive) ParseOutput(in string, m interface{}) (e error) {
	// to parse string std out to respective model

	if !toolkit.IsPointer(m) {
		return errorlib.Error("", "", "Fetch", "Model object should be pointer")
	}

	s := reflect.ValueOf(&m).Elem()
	typeOfT := s.Type()

	reader := csv.NewReader(strings.NewReader(in))
	record, e := reader.Read()

	if e != nil {
		return e
	}

	fmt.Println(s.Type())

	if s.NumField() != len(record) {
		return &FieldMismatch{s.NumField(), len(record)}
	}

	for i := 0; i < s.NumField(); i++ {
		head := h.Header[i]
		fieldName := typeOfT.Field(i).Name
		tag := s.Type().Field(i).Tag

		if (strings.ToUpper(fieldName) == strings.ToUpper(head)) ||
			(strings.ToUpper(fieldName) == strings.ToUpper(tag.Get("tag_name"))) {

			f := s.Field(i)
			switch f.Type().String() {
			case "string":
				f.SetString(record[i])
			case "int":
				var ival int64
				ival, e = strconv.ParseInt(record[i], 10, 0)
				if e != nil {
					return
				}
				f.SetInt(ival)
			default:
				e = &UnsupportedType{f.Type().String()}
				return
			}

		}

	}

	return
}*/

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
