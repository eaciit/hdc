package main

import (
	//"bufio"
	//"bytes"
	"fmt"
	"os"
	"os/exec"
	//"strings"
	//"reflect"
)

const (
	//ExecuteCommand = "beeline -u jdbc:hive2://"
	ConnString = "!connect jdbc:hive2://"
	LinuxCmd1  = "sh"
	LinuxCmd2  = "-c"
)

type HiveConfig struct {
	Server   string
	User     string
	Password string
	Database string
}

type HiveConnection struct {
	isAlive bool
}

type Hive struct {
	Config     HiveConfig
	Conn       HiveConnection
	BeeCommand string //eg : beeline or <absolute path>/beeline
}

func main() {

	var err error

	//initialize connection config
	Config := SetConfig("192.168.0.223:10000", "developer", "b1gD@T@", "default")
	Conn := HiveConnection{false}
	Host := Hive{Config, Conn, "beeline"}

	err = Host.OpenConnection(Host)
	status, err := Host.CheckConnection()
	err = Host.CloseConnection()

	if err != nil {
		fmt.Println(err)
	}

	if status == true {
		fmt.Println(status)
	}
}

func SetConfig(Server, User, Password, Database string) HiveConfig {
	Host := HiveConfig{}
	Host.Server = Server
	Host.User = User
	Host.Password = Password
	Host.Database = Database

	return Host
}

func (h *Hive) OpenConnection(Host Hive) (err error) {
	Conn := Host.Conn

	if !Conn.isAlive {
		cmd := exec.Command(Host.BeeCommand)
		//outpipe, err := cmd.StdoutPipe()

		if err != nil {
			ExitApp(err)
		}

		cmdx := exec.Command(makeCommand(Host.Config))
		cmdx.Stdin, err = cmd.StdoutPipe()

		err = cmd.Start()
		err = cmdx.Start()

		err = cmd.Wait()
		err = cmdx.Wait()

		output, err := cmdx.Output()
		fmt.Println(string(output))

		// cmd = exec.Command("echo", "aaaa")
		// outpipe, err = cmd.StdoutPipe()
		// err = cmd.Start()

		//err = cmd.Wait()

		if err != nil {
			ExitApp(err)
		}

		// cmd = exec.Command(LinuxCmd1, LinuxCmd2, makeCommand(Host.Config))
		// outpipe, err = cmd.StdoutPipe()
		// err = cmd.Start()

		// cmd.Wait()

		// fmt.Println(outpipe)

	}
	return nil
}

func (h *Hive) CheckConnection() (connstatus bool, err error) {
	return false, nil
}

func (h *Hive) CloseConnection() (err error) {
	return nil
}

func makeCommand(Config HiveConfig) (command string) {
	command = ConnString + Config.Server + "/" + Config.Database
	if Config.User != "" {
		command += " -n " + Config.User
	}

	if Config.Password != "" {
		command += " -p " + Config.Password
	}
	return command
}

func ExitApp(err error) {
	fmt.Println(err)
	os.Exit(1)
}
