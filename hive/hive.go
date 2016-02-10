package hive

import (
	"os/user"
)

type FnHiveReceive func(string) (interface{}, error)

type Hive struct {
	Server      string
	User        string
	Password    string
	HiveCommand string
}

func NewHiveConfig(server, userid, password string) *Hive {
	hv := Hive{}
	hv.Server = server
	hv.Password = password

	if userid == "" {
		user, err := user.Current()
		if err == nil {
			userid = user.Username
		}
	}

	hv.User = userid
	return &hv
}

func (h *Hive) Connect() error {
	return nil
}

func (h *Hive) Exec(query *string, fn FnHiveReceive) (hs *HiveSession, e error) {
	return
}

func (h *Hive) ExecFile(filepath *string, fn FnHiveReceive) (hs *HiveSession, e error) {
	return
}
