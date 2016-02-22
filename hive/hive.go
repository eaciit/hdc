package hive

import (
	"bufio"
	"fmt"
	"github.com/eaciit/errorlib"
	w "github.com/eaciit/hdc/worker"
	"github.com/eaciit/toolkit"
	"log"
	"os"
	"os/user"
	"reflect"
	"strconv"
	"strings"
)

const (
	BEE_TEMPLATE  = "%sbeeline -u jdbc:hive2://%s/%s"
	BEE_USER      = " -n %s"
	BEE_PASSWORD  = " -p %s"
	BEE_QUERY     = " -e \"%s\""
	PACKAGENAME   = "Hive"
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
	BeePath    string
	Server     string
	User       string
	Password   string
	DBName     string
	Conn       *DuplexTerm
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
			hv.Conn.CmdStr = hv.cmdStr(CSV_FORMAT)
		} else {
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
	return
}

func (h *Hive) Populate(query string, m interface{}) (e error) {
	if !toolkit.IsPointer(m) {
		e = errorlib.Error("", "", "Fetch", "Model object should be pointer")
		return
	}
	hr, e := h.fetch(query)

	if len(hr.Header) != 0 && len(hr.Result) > 2 {
		Parse(hr.Header, hr.Result, m, h.OutputType, "")
	}
	return
}

func (h *Hive) fetch(query string) (hr HiveResult, e error) {
	if !strings.HasPrefix(query, ";") {
		query += ";"
	}

	hr, e = h.Conn.SendInput(query)

	return
}

func (h *Hive) Exec(query string, fn FnHiveReceive) (e error) {
	delimiter := "\t"
	_ = delimiter

	if h.OutputType == CSV {
		delimiter = ","
	}

	if !strings.HasPrefix(query, ";") {
		query += ";"
	}

	h.Conn.FnReceive = fn
	_, e = h.Conn.SendInput(query)

	return
}

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
	hr, err := h.fetch("select '1' from " + TableName + " limit 1;")

	if err != nil {
		return retVal, err
	}

	if hr.Result == nil {
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

			_, err = h.fetch(tempQuery)

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
				if v.Field(i).Type.String() == "string" {
					insertValues += "\"" + reflect.ValueOf(TableModel).Elem().Field(i).String() + "\""
				} else if v.Field(i).Type.String() == "int" {
					temp, _ := strconv.ParseInt(reflect.ValueOf(TableModel).Elem().Field(i).String(), 32, 32)
					insertValues += strconv.FormatInt(temp, 10)
				} else if v.Field(i).Type.String() == "float" {
					insertValues += strconv.FormatFloat(reflect.ValueOf(TableModel).Elem().Field(i).Float(), 'f', 6, 64)
				} else {
					insertValues += "\"" + reflect.ValueOf(TableModel).Elem().Field(i).Interface().(string) + "\""
				}

				if i < v.NumField()-1 {
					insertValues += ", "
				}
			}

			if insertValues != "" {
				retVal := QueryBuilder("insert", TableName, insertValues, TableModel)
				_, err = h.fetch(retVal)
			}
		}

		if err == nil {
			retVal = "success"
		}
	}

	return retVal, err
}

func (h *Hive) LoadFile(FilePath, TableName, fileType string, TableModel interface{}) (retVal string, err error) {
	retVal = "process failed"
	isMatch := false
	hr, err := h.fetch("select '1' from " + TableName + " limit 1;")

	if err != nil {
		return retVal, err
	}

	if hr.Result == nil {
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
			_, err = h.fetch(tempQuery)
		}
	} else {
		isMatch, err = h.CheckDataStructure(TableName, TableModel)
	}

	if isMatch == false {
		return retVal, err
	}

	if err == nil {
		file, err := os.Open(FilePath)
		if err != nil {
			fmt.Println(err)
		}
		defer file.Close()

		if err != nil {
			log.Println(err)
		}

		scanner := bufio.NewScanner(file)

		//put depatcher here

		for scanner.Scan() {
			//put worker here

			err = Parse([]string{}, scanner.Text(), TableModel, "csv", "")

			if err != nil {
				log.Println(err)
			}

			insertValues := ""

			var v reflect.Type
			v = reflect.TypeOf(TableModel).Elem()

			if v.Kind() == reflect.Struct {
				for i := 0; i < v.NumField(); i++ {
					if v.Field(i).Type.String() == "string" {
						insertValues += "\"" + reflect.ValueOf(TableModel).Elem().Field(i).String() + "\""
					} else if v.Field(i).Type.String() == "int" {
						temp, _ := strconv.ParseInt(reflect.ValueOf(TableModel).Elem().Field(i).String(), 32, 32)
						insertValues += strconv.FormatInt(temp, 10)
					} else if v.Field(i).Type.String() == "float" {
						insertValues += strconv.FormatFloat(reflect.ValueOf(TableModel).Elem().Field(i).Float(), 'f', 6, 64)
					} else {
						insertValues += "\"" + reflect.ValueOf(TableModel).Elem().Field(i).Interface().(string) + "\""
					}

					if i < v.NumField()-1 {
						insertValues += ", "
					}
				}
			}

			if insertValues != "" {
				retQuery := QueryBuilder("insert", TableName, insertValues, TableModel)
				_, err = h.fetch(retQuery)
			}

		}

		if err == nil {
			retVal = "success"
		}
	}

	return retVal, err
}

// loading file with worker
func (h *Hive) LoadFileWithWorker(FilePath, TableName, fileType string, TableModel interface{}, TotalWorker int) (retVal string, err error) {
	retVal = "process failed"
	isMatch := false
	hr, err := h.fetch("select '1' from " + TableName + " limit 1;")

	if err != nil {
		return retVal, err
	}

	if hr.Result == nil {
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
			_, err = h.fetch(tempQuery)
		}
	} else {
		isMatch, err = h.CheckDataStructure(TableName, TableModel)
	}

	if isMatch == false {
		return retVal, err
	}

	if err == nil {
		file, err := os.Open(FilePath)
		if err != nil {
			fmt.Println(err)
		}
		defer file.Close()

		if err != nil {
			log.Println(err)
		}

		scanner := bufio.NewScanner(file)

		// initiate dispatcher
		manager := w.NewManager(TotalWorker, 1)

		// initiate workers
		for w := 0; w < TotalWorker; w++ {
			manager.FreeWorkers <- &w.Worker{w, manager.TimeProcess, manager.FreeWorkers}
		}

		// monitoring worker whos free
		go manager.DoMonitor()

		for scanner.Scan() {
			// get data to parse into task
			retVal := QueryBuilder("insert", TableName, scanner.Text(), Parse([]string{}, scanner.Text(), &TableModel, "csv", ""))

			// do task with worker
			manager.Tasks <- func() {
				hr, err = h.fetch(retVal)
			}
		}

		// waiting for tasks has been done
		go manager.Timeout(1)
		<-manager.Done

		if err == nil {
			retVal = "success"
		}
	}

	return retVal, err
}

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
				if hr.Result != nil {
					line := strings.Split(strings.Replace(hr.Result[i], "'", "", -1), "\t")
					var tempDataType = ""

					if strings.TrimSpace(line[1]) == "double" {
						tempDataType = "float"
					} else if strings.TrimSpace(line[1]) == "varchar(64)" {
						tempDataType = "string"
					} else {
						tempDataType = strings.TrimSpace(line[1])
					}

					if tempDataType == v.Field(i).Type.String() {
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
				retVal += input + ");"
				break
			} else if clause == "ADD COLUMN" {
				retVal += reflect.ValueOf(TableModel).Elem().Field(i).String() + " " + v.Field(i).Type.String()
			}

			if i < v.NumField()-1 {
				retVal += ","
			} else {
				retVal += ");"
			}
		}
	}
	return retVal
}
