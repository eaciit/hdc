package hive

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/eaciit/cast"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"github.com/metakeule/fmtdate"
	"os"
	"os/exec"
	"os/user"
	"reflect"
	"regexp"
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

type FnHiveReceive func(string) (interface{}, error)

type Hive struct {
	BeePath     string
	Server      string
	User        string
	Password    string
	DBName      string
	HiveCommand string
	Header      []string
	OutputType  string
	DateFormat  string
	JsonPart    string
	Conn        DuplexTerm
}

func HiveConfig(server, dbName, userid, password, path string, delimiter ...string) *Hive {
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

	hv.User = userid

	hv.OutputType = "tsv"
	if len(delimiter) > 0 && delimiter[0] == "csv" {
		hv.OutputType = "csv"
	}

	hv.Conn = DuplexTerm{}

	if hv.Conn.Cmd == nil {
		if hv.OutputType == "csv" {
			hv.Conn.Cmd = hv.command(hv.cmdStr(CSV_FORMAT))
		} else {
			hv.Conn.Cmd = hv.command(hv.cmdStr(TSV_FORMAT))
		}
	}

	return &hv
}

/*func SetHeader(header []string) *Hive {
	hv := Hive{}
	hv.Header = header
	return &hv
}*/

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

	if h.HiveCommand != "" {
		out += fmt.Sprintf(BEE_QUERY, h.HiveCommand)
	}
	return
}

func (h *Hive) command(cmd ...string) *exec.Cmd {
	arg := append([]string{"-c"}, cmd...)
	return exec.Command("sh", arg...)
}

func (h *Hive) constructHeader(header string, delimiter string) {
	var tmpHeader []string
	for _, header := range strings.Split(header, delimiter) {
		split := strings.Split(header, ".")
		if len(split) > 1 {
			tmpHeader = append(tmpHeader, strings.Trim(split[1], " '"))
		} else {
			tmpHeader = append(tmpHeader, strings.Trim(header, " '"))
		}
	}
	h.Header = tmpHeader
}

func (h *Hive) Exec(query string) (out []string, e error) {
	h.HiveCommand = query
	delimiter := "\t"

	if h.OutputType == "csv" {
		delimiter = ","
	}

	if !strings.HasPrefix(query, ";") {
		query += ";"
	}

	result, e := h.Conn.SendInput(query)

	if e != nil {
		return
	}

	if len(result) > 0 {
		h.constructHeader(result[:1][0], delimiter)
	}

	if len(result) > 1 {
		out = result[1:]
	}
	return
}

/*func (h *Hive) Exec(query string) (out []string, e error) {
	h.HiveCommand = query
	cmd := h.command()

	delimiter := "\t"
	if h.OutputType == "csv" {
		cmd = h.command(h.cmdStr(CSV_FORMAT))
		delimiter = ","
	} else {
		cmd = h.command(h.cmdStr(TSV_FORMAT))
	}

	outByte, e := cmd.Output()
	result := strings.Split(string(outByte), "\n")

	if len(result) > 0 {
		h.constructHeader(result[:1][0], delimiter)
	}

	//fmt.Printf("header: %v\n", h.Header)

	if len(result) > 1 {
		out = result[1:]
	}
	return
}*/

func (h *Hive) ExecLine(query string, DoResult func(result string)) (e error) {
	h.HiveCommand = query
	cmd := h.command()

	delimiter := "\t"
	if h.OutputType == "csv" {
		cmd = h.command(h.cmdStr(CSV_FORMAT))
		delimiter = ","
	} else {
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
				h.constructHeader(resStr, delimiter)
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

	if h.OutputType == "csv" {
		var v reflect.Type
		v = reflect.TypeOf(m).Elem()
		ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

		appendData := toolkit.M{}
		iv := reflect.New(v).Interface()
		reader := csv.NewReader(strings.NewReader(""))
		if strings.Contains(in, "','") {
			reader = csv.NewReader(strings.NewReader("\"" + strings.Trim(strings.Replace(in, "','", "\",\"", -1), "'") + "\""))
		} else {
			reader = csv.NewReader(strings.NewReader(in))
		}
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
					valthis := appendData[v.Field(i).Name]
					if valthis == nil {
						valthis = appendData[tag.Get("tag_name")]
					}

					switch v.Field(i).Type.Kind() {
					case reflect.Int:
						appendData.Set(v.Field(i).Name, cast.ToInt(valthis, cast.RoundingAuto))
					case reflect.Float32:
						valf, _ := strconv.ParseFloat(valthis.(string), 32)
						appendData.Set(v.Field(i).Name, valf)
					case reflect.Float64:
						valf, _ := strconv.ParseFloat(valthis.(string), 64)
						appendData.Set(v.Field(i).Name, valf)
					}

					dtype := h.DetectFormat(valthis.(string))
					if dtype == "date" {
						valf, _ := fmtdate.Parse(h.DateFormat, valthis.(string))
						appendData.Set(v.Field(i).Name, valf)
					} else if dtype == "bool" {
						valf, _ := strconv.ParseBool(valthis.(string))
						appendData.Set(v.Field(i).Name, valf)
					}
				}
			}
		} else {
			for _, val := range h.Header {
				valthis := appendData[val]
				dtype := h.DetectFormat(valthis.(string))
				if dtype == "int" {
					appendData.Set(val, cast.ToInt(valthis, cast.RoundingAuto))
				} else if dtype == "float" {
					valf, _ := strconv.ParseFloat(valthis.(string), 64)
					appendData.Set(val, valf)
				} else if dtype == "date" {
					valf, _ := fmtdate.Parse(h.DateFormat, valthis.(string))
					appendData.Set(val, valf)
				} else if dtype == "bool" {
					valf, _ := strconv.ParseBool(valthis.(string))
					appendData.Set(val, valf)
				}
			}
		}

		toolkit.Serde(appendData, iv, "json")
		ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())
		reflect.ValueOf(m).Elem().Set(ivs.Index(0))
	} else if h.OutputType == "json" {
		var temp = toolkit.M{}
		in = h.InspectJson(in)
		if in != "" {
			e := json.Unmarshal([]byte(in), &temp)
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
					valthis := appendData[v.Field(i).Name]
					if valthis == nil {
						valthis = appendData[tag.Get("tag_name")]
					}
					switch v.Field(i).Type.Kind() {
					case reflect.Int:
						appendData.Set(v.Field(i).Name, cast.ToInt(valthis, cast.RoundingAuto))
					case reflect.Float32:
						valf, _ := strconv.ParseFloat(valthis.(string), 32)
						appendData.Set(v.Field(i).Name, valf)
					case reflect.Float64:
						valf, _ := strconv.ParseFloat(valthis.(string), 64)
						appendData.Set(v.Field(i).Name, valf)
					}
					dtype := h.DetectFormat(valthis.(string))
					if dtype == "date" {
						valf, _ := fmtdate.Parse(h.DateFormat, valthis.(string))
						appendData.Set(v.Field(i).Name, valf)
					} else if dtype == "bool" {
						valf, _ := strconv.ParseBool(valthis.(string))
						appendData.Set(v.Field(i).Name, valf)
					}
				}
			}

		} else {
			for _, val := range h.Header {
				valthis := appendData[val]
				dtype := h.DetectFormat(valthis.(string))
				if dtype == "int" {
					appendData.Set(val, cast.ToInt(valthis, cast.RoundingAuto))
				} else if dtype == "float" {
					valf, _ := strconv.ParseFloat(valthis.(string), 64)
					appendData.Set(val, valf)
				} else if dtype == "date" {
					valf, _ := fmtdate.Parse(h.DateFormat, valthis.(string))
					appendData.Set(val, valf)
				} else if dtype == "bool" {
					valf, _ := strconv.ParseBool(valthis.(string))
					appendData.Set(val, valf)
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

func (h *Hive) InspectJson(in string) (out string) {
	if h.JsonPart != "" {
		in = h.JsonPart + in
	}
	res := ""
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
				h.JsonPart = ""
			} else {
				h.JsonPart = in[i+1:]
			}
			res = in[:i+1]
			break
		}
	}

	if charopen != charclose || (charclose == 0 && charopen == 0) {
		h.JsonPart = in
	}

	return strings.Trim(strings.TrimSpace(res), " ,")
}

func (h *Hive) DetectFormat(in string) (out string) {
	res := ""
	if in != "" {
		matchNumber := false
		matchFloat := false
		matchDate := false

		formatDate := "((^(0[0-9]|[0-9]|(1|2)[0-9]|3[0-1])(\\.|\\/|-)(0[0-9]|[0-9]|1[0-2])(\\.|\\/|-)[\\d]{4}$)|(^[\\d]{4}(\\.|\\/|-)(0[0-9]|[0-9]|1[0-2])(\\.|\\/|-)(0[0-9]|[0-9]|(1|2)[0-9]|3[0-1])$))"
		matchDate, _ = regexp.MatchString(formatDate, in)

		if !matchDate && h.DateFormat != "" {
			d := cast.String2Date(in, h.DateFormat)
			if d.Year() > 1 {
				matchDate = true
			} else {
				d, e := fmtdate.Parse(h.DateFormat, in)
				if e == nil || d.Year() > 1 {
					matchDate = true
				}
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
