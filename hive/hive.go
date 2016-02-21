package hive

import (
	"bufio"
	"fmt"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"log"
	"os"
	// "os/exec"
	"os/user"
	"reflect"
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
	JSON          = "json"
)

type FnHiveReceive func(HiveResult) error

type Hive struct {
	BeePath  string
	Server   string
	User     string
	Password string
	DBName   string
	Conn     *DuplexTerm
	// HiveCommand string
	// Header      []string
	OutputType string
	DateFormat string
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

	hv.Conn = &DuplexTerm{}

	hv.OutputType = TSV
	hv.Conn.OutputType = TSV
	if len(delimiter) > 0 && delimiter[0] == CSV {
		hv.OutputType = CSV
		hv.Conn.OutputType = TSV
	}

	if hv.Conn.Cmd == nil {
		if hv.OutputType == CSV {
			//hv.Conn.Cmd = hv.command(hv.cmdStr(CSV_FORMAT))
			hv.Conn.CmdStr = hv.cmdStr(CSV_FORMAT)
		} else {
			// hv.Conn.Cmd = hv.command(hv.cmdStr(TSV_FORMAT))
			hv.Conn.CmdStr = hv.cmdStr(TSV_FORMAT)
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

	/*if h.HiveCommand != "" {
		out += fmt.Sprintf(BEE_QUERY, h.HiveCommand)
	}*/
	return
}

func (h *Hive) Populate(query string, m interface{}) (e error) {
	if !toolkit.IsPointer(m) {
		e = errorlib.Error("", "", "Fetch", "Model object should be pointer")
		return
	}
	hr, e := h.fetch(query)

	log.Println(hr)

	Parse(hr.Header, hr.Result[1:], m, h.OutputType, "")
	return
}

func (h *Hive) fetch(query string) (hr HiveResult, e error) {
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

	hr.Result = result

	if len(hr.Result) > 0 {
		hr.constructHeader(hr.Result[:1][0], delimiter)
	}
	return
}

func (h *Hive) Exec(query string) {
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

/*func (h *Hive) ExecFile(filepath string) (e error) {
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
}*/

func (h *Hive) ImportHDFS(HDFSPath, TableName, Delimiter string, TableModel interface{}) (retVal string, err error) {
	retVal = "process failed"
	hr, err := h.fetch("select '1' from " + TableName + " limit 1;")

	if hr.Result == nil {
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
			hr, err = h.fetch(tempQuery)
		}
	}

	if err != nil {
		hr, err = h.fetch("load data local inpath '" + HDFSPath + "' overwrite into table " + TableName + ";")

		if err != nil {
			retVal = "success"
		}
	}

	return retVal, err
}

func (h *Hive) Load(TableName, Delimiter string, TableModel interface{}) (retVal string, err error) {
	retVal = "process failed"
	isMatch := false
	log.Println(TableName)
	log.Println(TableModel)
	var hr []toolkit.M
	err = h.Populate("select '1' from "+TableName+" limit 1;", &hr)

	log.Println(hr)

	if err != nil {
		return retVal, err
	}

	fmt.Println("tempVal2")

	if hr == nil {
		tempQuery := ""

		var v reflect.Type
		v = reflect.TypeOf(TableModel).Elem()

		if v.Kind() == reflect.Struct {
			tempQuery = "create table " + TableName + " ("
			for i := 0; i < v.NumField(); i++ {
				if i == (v.NumField() - 1) {
					tempQuery += v.Field(i).Name + " " + v.Field(i).Type.String() + ");"
				} else {
					tempQuery += v.Field(i).Name + " " + v.Field(i).Type.String() + ", "
				}
			}

			var hr1 []toolkit.M
			log.Println(tempQuery)
			err = h.Populate(tempQuery, &hr1)
		}
	} else {
		isMatch, err = h.CheckDataStructure(TableName, TableModel)
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
			_, err = h.fetch(retVal)
		}

		if err == nil {
			retVal = "success"
		}
	}

	return retVal, err
}

func (h *Hive) LoadFile(HDFSPath, TableName, fileType string, TableModel interface{}) (retVal string, err error) {
	retVal = "process failed"
	isMatch := false
	hr, err := h.fetch("select '1' from " + TableName + " limit 1")

	if hr.Result == nil {
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
			hr, err = h.fetch(tempQuery)
		}
	} else {
		isMatch, err = h.CheckDataStructure(TableName, &TableModel)
	}

	if isMatch == false {
		return retVal, err
	}

	if err == nil {
		file, err := os.Open(HDFSPath)
		if err != nil {
			fmt.Println(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			err = Parse(nil, scanner.Text(), TableModel, fileType, h.DateFormat)

			if err != nil {
				fmt.Println(err)
				break
			}

			retVal := QueryBuilder("insert", TableName, scanner.Text(), Parse(nil, scanner.Text(), TableModel, fileType, h.DateFormat))
			hr, err = h.fetch(retVal)
		}

		if err == nil {
			retVal = "success"
		}
	}

	return retVal, err
}

// func (h *Hive) CheckDataStructure(Tablename, Delimiter string, TableModel interface{}) (isMatch bool, err error) {
func (h *Hive) CheckDataStructure(Tablename string, TableModel interface{}) (isMatch bool, err error) {
	isMatch = false
	hr, err := h.fetch("describe " + Tablename + ";")

	if err != nil {
		return isMatch, err
	}

	if hr.Result != nil {
		var v reflect.Type
		v = reflect.TypeOf(TableModel).Elem()

		if v.Kind() == reflect.Struct {
			for i := 0; i < v.NumField(); i++ {
				if hr.Result[i] != "" {
					lines := strings.Split(hr.Result[i], ",")
					if strings.Replace(strings.TrimSpace(lines[1]), "double", "float", 0) == v.Field(i).Type.String() {
						isMatch = true
					} else {
						isMatch = false
						break
					}
				} else {
					// handle new column
					_, err := h.fetch(QueryBuilder("add column", Tablename, "", TableModel))

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
