package hdfs

import (
	"fmt"
	//. "github.com/eaciit/hdc/hdfs"
	. "github.com/RyanCi/hdc/hdfs"
	"os"
	"testing"
	"time"
)

func killApp(code int) {
	os.Exit(code)
}

var h *WebHdfs
var e error

func TestConnect(t *testing.T) {
	h, e = NewWebHdfs(NewHdfsConfig("http://192.168.0.223:50070", "hdfs"))
	if e != nil {
		t.Fatalf(e.Error())
		defer killApp(1000)
	}
	h.Config.TimeOut = 2 * time.Millisecond
	h.Config.PoolSize = 100
}

func TestDelete(t *testing.T) {
	if es := h.Delete(true, "/user/ariefdarmawan"); es != nil {
		t.Errorf("%s", func() string {
			s := ""
			for k, e := range es {
				s += fmt.Sprintf("%s = %s", k, e.Error())
			}
			return s
		}())
	}
}

func TestCreateDir(t *testing.T) {
	es := h.MakeDirs([]string{"/user/ariefdarmawan/inbox", "/user/ariefdarmawan/temp", "/user/ariefdarmawan/outbox"}, "")
	if es != nil {
		for k, v := range es {
			t.Error(fmt.Sprintf("Error when create %v : %v \n", k, v))
		}
	}
}

func TestChangeOwner(t *testing.T) {
	if e = h.SetOwner("/user/ariefdarmawan", "ariefdarmawan", ""); e != nil {
		t.Error(e.Error())
	}
}

/*
	fmt.Println(">>>> TEST COPY DIR <<<<")
	e, es = h.PutDir("/Users/ariefdarmawan/Temp/ECFZ/TempVisa/JSON", "/user/ariefdarmawan/inbox/ecfz/json")
	if es != nil {
		for k, v := range es {
			t.Error(fmt.Sprintf("Error when create %v : %v \n", k, v))
		}
	}
*/

func TestPutFile(t *testing.T) {
	//e = h.Put("d://test.txt", "/user/ariefdarmawan/inbox/test.txt", "", nil)
	e = h.Put("/home/developer/test.txt", "/user/ariefdarmawan/inbox/test.txt", "", nil)
	if e != nil {
		t.Error(e.Error())
	}
}

func TestGetStatus(t *testing.T) {
	hdata, e := h.List("/user/ariefdarmawan")
	if e != nil {
		t.Error(e.Error())
	} else {
		fmt.Printf("Data Processed :\n%v\n", len(hdata.FileStatuses.FileStatus))
	}
}

func TestSetPermission(t *testing.T) {
	e = h.SetPermission("/user/ariefdarmawan/inbox/test.txt", "777")
	if e != nil {
		t.Error(e.Error())
	}
}

func TestCreateNewFile(t *testing.T) {
	fmt.Println(os.Getenv("HOME"))
	e = h.CreateNewFile("/user/ariefdarmawan/inbox/", "text2.txt", "755")
	if e != nil {
		t.Error(e.Error())
	}
}
