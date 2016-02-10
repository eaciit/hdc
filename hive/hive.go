package hive

import (
	"fmt"
	// "log"
	"bufio"
	"os"
	"os/exec"
	"os/user"
)

type FnHiveReceive func(string) (interface{}, error)

type Hive struct {
	Server      string
	User        string
	Password    string
	DBName      string
	HiveCommand string
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

func (h *Hive) ParseOutput(stdout string) (out interface{},e error) {
	// to parse string std out to respective model
	return nil,nil
}

func (h *Hive) ExecNonQuery(query string) (e error) {
	cmdStr := "beeline -u jdbc:hive2://" + h.Server + "/" + h.DBName + " -n " + h.User + " -p " + h.Password + " -e " + "\"" + query + "\""

	cmd := exec.Command("sh", "-c", cmdStr)
	out, err := cmd.Output()
	if err == nil{
		fmt.Printf("result: %s\n", out)
	}else{
		fmt.Printf("result: %s\n", err)
	}
	return err
}

func (h *Hive) Exec(query string, fn FnHiveReceive) (hs *HiveSession, e error) {
	cmdStr := "beeline -u jdbc:hive2://" + h.Server + "/" + h.DBName + " -n " + h.User + " -p " + h.Password + " -e " + "\"" + query + "\""

	cmd := exec.Command("sh", "-c", cmdStr)
	out, err := cmd.Output()
	fmt.Printf("result: %s\n", out)
	_ = err
	return nil, nil
}

func (h *Hive) ExecFile(filepath string, fn FnHiveReceive) (hs *HiveSession, e error) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
		h.Exec(scanner.Text(), nil)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	return nil, nil
}
