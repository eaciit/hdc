package hive

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/eaciit/cast"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	// "log"
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
	PACKAGENAME  = "Hive"

	/*SHOW_HEADER  = " --showHeader=true"
	HIDE_HEADER  = " --showHeader=false"*/
	CSV_FORMAT    = " --outputFormat=csv"
	TSV_FORMAT    = " --outputFormat=tsv"
	DSV_FORMAT    = " --outputFormat=dsv --delimiterForDSV=|\t"
	DSV_DELIMITER = "|\t"
	TSV           = "tsv"
	CSV           = "csv"
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

	hv.OutputType = TSV
	if len(delimiter) > 0 && delimiter[0] == CSV {
		hv.OutputType = CSV
	}

	hv.Conn = DuplexTerm{}

	if hv.Conn.Cmd == nil {
		if hv.OutputType == CSV {
			hv.Conn.Cmd = hv.command(hv.cmdStr(CSV_FORMAT))
		} else {
			hv.Conn.Cmd = hv.command(hv.cmdStr(TSV_FORMAT))
		}
	}

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
	delimiter := "\t"

	if h.OutputType == CSV {
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

func (h *Hive) Populate(query string, m interface{}) (e error) {
	if !toolkit.IsPointer(m) {
		return errorlib.Error("", "", "Fetch", "Model object should be pointer")
	}

	/*

		var v reflect.Type
		v = reflect.TypeOf(m).Elem()
		log.Printf("v: %v\n", v)
		ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

		appendData := toolkit.M{}
		iv := reflect.New(v).Interface()*/

	delimiter := "\t"

	if h.OutputType == CSV {
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

	h.ParseOutput(result[1:], m)

	/*if len(result) > 1 {
		rows := result[1:]
		for _, val := range rows {
			h.ParseOutput(val, m)
			out = append(out, obj)
		}

	}*/

	return
}

/*func (h *Hive) Exec(query string) (out []string, e error) {
	h.HiveCommand = query
	cmd := h.command()

	delimiter := "\t"
	if h.OutputType == CSV {
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

func (h *Hive) ExecLineX(query string) {
	delimiter := "\t"
	_ = delimiter

	if h.OutputType == CSV {
		delimiter = ","
	}

	if !strings.HasPrefix(query, ";") {
		query += ";"
	}

	_, e := h.Conn.SendInput(query)

	if e != nil {
		return
	}

	return
}

func (h *Hive) ExecLine(query string, DoResult func(result string)) (e error) {
	h.HiveCommand = query
	cmd := h.command()

	delimiter := "\t"
	if h.OutputType == CSV {
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

func (h *Hive) Load(TableName, Delimiter string, TableModel interface{}) (retVal string, err error) {
	retVal = "process failed"
	isMatch := false
	tempVal, err := h.Exec("select '1' from " + TableName + " limit 1")

	if tempVal == nil {
		tempQuery := ""

		var v reflect.Type
		v = reflect.TypeOf(TableModel).Elem()

		if v.Kind() == reflect.Struct {
			tempQuery = "create table " + TableName + " ("
			for i := 0; i < v.NumField(); i++ {
				if i == (v.NumField() - 1) {
					tempQuery += v.Field(i).Name + " " + v.Field(i).Type.String() + ") "
				} else {
					tempQuery += v.Field(i).Name + " " + v.Field(i).Type.String() + ", "
				}
			}
			tempVal, err = h.Exec(tempQuery)
		}
	} else {
		isMatch, err = h.CheckDataStructure(TableName, Delimiter, TableModel)
	}

	if isMatch == false {
		return retVal, err
	}

	if err == nil {
		insertValues := ""

		var v reflect.Type
		v = reflect.TypeOf(TableModel).Elem()

		if v.Kind() == reflect.Struct {
			for i := 0; i < v.NumField(); i++ {
				if i == (v.NumField() - 1) {
					insertValues += reflect.ValueOf(TableModel).Field(i).String() + ")"
				} else {
					insertValues += reflect.ValueOf(TableModel).Field(i).String() + ", "
				}
			}
			retVal := QueryBuilder("insert", TableName, insertValues, TableModel)
			_, err = h.Exec(retVal)
		}

		if err == nil {
			retVal = "success"
		}
	}

	return retVal, err
}

func (h *Hive) LoadFile(HDFSPath, TableName, Delimiter string, TableModel interface{}) (retVal string, err error) {
	retVal = "process failed"
	isMatch := false
	tempVal, err := h.Exec("select '1' from " + TableName + " limit 1")

	if tempVal == nil {
		tempQuery := ""

		var v reflect.Type
		v = reflect.TypeOf(TableModel).Elem()

		if v.Kind() == reflect.Struct {
			tempQuery = "create table " + TableName + " ("
			for i := 0; i < v.NumField(); i++ {
				if i == (v.NumField() - 1) {
					tempQuery += v.Field(i).Name + " " + v.Field(i).Type.String() + ") "
				} else {
					tempQuery += v.Field(i).Name + " " + v.Field(i).Type.String() + ", "
				}
			}
			tempVal, err = h.Exec(tempQuery)
		}
	} else {
		isMatch, err = h.CheckDataStructure(TableName, Delimiter, TableModel)
	}

	if isMatch == false {
		return retVal, err
	}

	if err == nil {
		file, e := os.Open(HDFSPath)
		if e != nil {
			fmt.Println(e)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			err = h.ParseOutput(scanner.Text(), TableModel)

			if err != nil {
				fmt.Println(err)
				break
			}

			retVal := QueryBuilder("insert", TableName, scanner.Text(), h.ParseOutput(scanner.Text(), TableModel))
			_, err = h.Exec(retVal)
		}

		if err == nil {
			retVal = "success"
		}
	}

	return retVal, err
}

func (h *Hive) CheckDataStructure(Tablename, Delimiter string, TableModel interface{}) (isMatch bool, err error) {
	isMatch = false
	res, err := h.Exec("describe " + Tablename + ";")

	if err != nil {
		return isMatch, err
	}

	if res != nil {
		var v reflect.Type
		v = reflect.TypeOf(TableModel).Elem()

		if v.Kind() == reflect.Struct {
			for i := 0; i < v.NumField(); i++ {
				if res[i] != "" {
					lines := strings.Split(res[i], Delimiter)

					if strings.Replace(strings.TrimSpace(lines[1]), "double", "float", 0) == v.Field(i).Type.String() {
						isMatch = true
					} else {
						isMatch = false
						break
					}
				} else {
					// handle new column
					_, err := h.Exec(QueryBuilder("add column", Tablename, "", TableModel))

					if err != nil {
						break
					}

					isMatch = true
				}
			}
		}
	}
	return isMatch, err
}

func QueryBuilder(clause, tablename, input string, TableModel interface{}) (retVal string) {
	clause = strings.ToUpper(clause)
	retVal = ""

	if clause == "INSERT" {
		retVal += clause + " INTO " + tablename + " VALUES ("
	} else if clause == "ADD COLUMN" {
		retVal += "ALTER TABLE" + tablename + " ADD COLUMNS ("
	} else if clause == "SELECT" {
		retVal += "SELECT * FROM " + tablename + ";"
	}

	var v reflect.Type
	v = reflect.TypeOf(TableModel).Elem()

	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			if clause == "INSERT" {
				retVal += reflect.ValueOf(TableModel).Field(i).String()
			} else if clause == "ADD COLUMN" {
				retVal += reflect.ValueOf(TableModel).Field(i).String() + " " + v.Field(i).Type.String()
			}

			if i < v.NumField()-1 {
				retVal += ","
			} else {
				retVal += ")"
			}
		}
	}

	return retVal
}

func (h *Hive) ParseOutput(in interface{}, m interface{}) (e error) {

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

	if h.OutputType == CSV {
		var v reflect.Type

		if slice {
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
							valf := cast.String2Date(valthis.(string), h.DateFormat)
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
						valf := cast.String2Date(valthis.(string), h.DateFormat)
						appendData.Set(val, valf)
					} else if dtype == "bool" {
						valf, _ := strconv.ParseBool(valthis.(string))
						appendData.Set(val, valf)
					}
				}
			}

			toolkit.Serde(appendData, iv, "json")
			ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())
		}
		if slice {
			reflect.ValueOf(m).Elem().Set(ivs)
		} else {
			reflect.ValueOf(m).Elem().Set(ivs.Index(0))
		}
	} else if h.OutputType == "json" {
		var temp interface{}
		ins = h.InspectJson(ins)

		//for catch multi json in one line
		if h.JsonPart != "" && slice {
			for {
				tempjsonpart := h.JsonPart
				h.JsonPart = ""
				tempIn := h.InspectJson([]string{tempjsonpart})
				if len(tempIn) == 0 {
					break
				} else {
					for _, tin := range tempIn {
						ins = append(ins, tin)
					}
				}
			}
		}

		inss := fmt.Sprintf("[%s]", strings.Join(ins, ","))
		if len(ins) > 0 {
			e := json.Unmarshal([]byte(inss), &temp)
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

		if slice {
			v = reflect.TypeOf(m).Elem().Elem()
		} else {
			v = reflect.TypeOf(m).Elem()
		}

		ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

		for _, data := range ins {
			appendData := toolkit.M{}
			iv := reflect.New(v).Interface()

			splitted := strings.Split(data, "\t")

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
							valf := cast.String2Date(valthis.(string), h.DateFormat)
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
						valf := cast.String2Date(valthis.(string), h.DateFormat)
						appendData.Set(val, valf)
					} else if dtype == "bool" {
						valf, _ := strconv.ParseBool(valthis.(string))
						appendData.Set(val, valf)
					}
				}
			}

			toolkit.Serde(appendData, iv, "json")
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

func (h *Hive) InspectJson(ins []string) (out []string) {
	var re []string

	for _, in := range ins {
		if h.JsonPart != "" {
			in = h.JsonPart + in
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
					h.JsonPart = ""
				} else {
					h.JsonPart = in[i+1:]
				}
				re = append(re, strings.Trim(strings.TrimSpace(in[:i+1]), " ,"))
				break
			}
			if charopen != charclose || (charclose == 0 && charopen == 0) {
				h.JsonPart = in
			}
		}

	}
	return re
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
