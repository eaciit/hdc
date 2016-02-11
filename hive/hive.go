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

const beeTemplate = "beeline -u jdbc:hive2://%s/%s -n %s -p %s -e \"%s\""

func (h *Hive) cmdStr() string {
	/*cmdStr := "beeline -u jdbc:hive2://" + h.Server + "/" + h.DBName + " -n " + h.User + " -p " + h.Password + " -e " + "\"" + query + "\""*/
	return fmt.Sprintf(beeTemplate, h.Server, h.DBName, h.User, h.Password, h.HiveCommand)
}

func (h *Hive) command(cmd ...string) *exec.Cmd {
	arg := append(
		[]string{
			"-c",
		},
		cmd...,
	)
	return exec.Command("sh", arg...)
}

func (h *Hive) Exec(query string) (out []byte, e error) {
	h.HiveCommand = query
	//cmd := exec.Command("sh", "-c", h.cmdStr())
	cmd := h.command(h.cmdStr())
	out, e = cmd.Output()
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

func (h *Hive) ParseOutput(stdout string, m interface{}) (out interface{}, e error) {
	// to parse string std out to respective model
	return nil, nil
}
