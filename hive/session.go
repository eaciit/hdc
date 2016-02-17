package hive

type HiveSession struct {
	result    chan string
	FnReceive FnHiveReceive
	Status    string
}

func (hs *HiveSession) receive(s string) (ret interface{}, err error) {
	return hs.FnReceive(s)
}

func (hs *HiveSession) Wait() {
	hs.Status = "Processing"
	//--- wait until first data is retrieved or completed

	hs.Status = "Receiving"

	//--- receive channel and for every receive run FnReceive
	//--- loop this process until session is completed

	hs.Status = "Successfull"
}
