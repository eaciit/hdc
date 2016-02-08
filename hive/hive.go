package hive

type FnHiveReceive func(string) (interface{}, error)

type Hive struct {
	Server      string
	User        string
	Password    string
	HiveCommand string
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
