package hdfs

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/user"
	"strings"
	"time"
)

// Constant
const WebHdfsApi string = "/webhdfs/v1/"
const (
	OP_OPEN                  = "OPEN"
	OP_CREATE                = "CREATE"
	OP_APPEND                = "APPEND"
	OP_CONCAT                = "CONCAT"
	OP_RENAME                = "RENAME"
	OP_DELETE                = "DELETE"
	OP_SETPERMISSION         = "SETPERMISSION"
	OP_SETOWNER              = "SETOWNER"
	OP_SETREPLICATION        = "SETREPLICATION"
	OP_SETTIMES              = "SETTIMES"
	OP_MKDIRS                = "MKDIRS"
	OP_CREATESYMLINK         = "CREATESYMLINK"
	OP_LISTSTATUS            = "LISTSTATUS"
	OP_GETFILESTATUS         = "GETFILESTATUS"
	OP_GETCONTENTSUMMARY     = "GETCONTENTSUMMARY"
	OP_GETFILECHECKSUM       = "GETFILECHECKSUM"
	OP_GETDELEGATIONTOKEN    = "GETDELEGATIONTOKEN"
	OP_GETDELEGATIONTOKENS   = "GETDELEGATIONTOKENS"
	OP_RENEWDELEGATIONTOKEN  = "RENEWDELEGATIONTOKEN"
	OP_CANCELDELEGATIONTOKEN = "CANCELDELEGATIONTOKEN"
)

type WebHdfsConfig struct {
	Host     string
	UserId   string
	Password string
	Token    string
	Method   string
	TimeOut  time.Duration
	PoolSize int
}

type WebHdfs struct {
	Config *WebHdfsConfig
	client *http.Client
}

func NewHdfsConfig(host, userid string) *WebHdfsConfig {
	cfg := WebHdfsConfig{}
	cfg.TimeOut = time.Second * 15
	cfg.Host = host

	if userid == "" {
		user, err := user.Current()
		if err == nil {
			userid = user.Username
		}
	}
	cfg.UserId = userid
	cfg.PoolSize = 5
	return &cfg
}

func NewWebHdfs(config *WebHdfsConfig) (*WebHdfs, error) {
	hdfs := new(WebHdfs)
	hdfs.Config = config

	hdfs.client = &http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				c, err := net.DialTimeout(netw, addr, config.TimeOut)
				if err != nil {
					return nil, err
				}
				return c, nil
			},
		},
	}
	return hdfs, nil
}

func (h *WebHdfs) makePath(path string, op string, parms map[string]string) string {
	s := h.Config.Host
	s = s + WebHdfsApi
	if path[0] == '/' {
		path = path[1:]
	}
	s = s + path
	if op != "" {
		s = s + "?op=" + op
	}
	s = s + "&user.name=" + h.Config.UserId
	for k, v := range parms {
		s += "&" + k + "=" + v
	}
	return s
}

/*func (h *WebHdfs) call(calltype, path, op string, parms map[string]string) (*http.Response, error) {
	url := ""
	if strings.HasPrefix(path, "http") == false {
		url = h.makePath(path, op, parms)
	} else {
		url = path
	}
	//return nil, errors.New(url)

	req, err := http.NewRequest(calltype, url, nil)
	if err != nil {
		return nil, err
	}
	return h.client.Do(req)
}*/

func (h *WebHdfs) call(calltype, path, op string, parms map[string]string) (*http.Response, error) {
	url := ""
	if strings.HasPrefix(path, "http") == false {
		url = h.makePath(path, op, parms)
	} else {
		url = path
	}
	//return nil, errors.New(url)

	timeout := time.Duration(5 * time.Second)
	req, err := http.NewRequest(calltype, url, nil)
	client := http.Client{
		Timeout: timeout,
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return res, err
}

/*func (h *WebHdfs) callPayload(calltype, path, op string, filename string, parms map[string]string) (*http.Response, error) {
	url := ""
	if strings.HasPrefix(path, "http") == false {
		url = h.makePath(path, op, parms)
	} else {
		url = path
	}

	payload, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer payload.Close()

	req, err := http.NewRequest(calltype, url, payload)
	if err != nil {
		return nil, err
	}
	return h.client.Do(req)
}*/

func (h *WebHdfs) callPayload(calltype, path, op string, filename string, parms map[string]string) (*http.Response, error) {
	url := ""
	if strings.HasPrefix(path, "http") == false {
		url = h.makePath(path, op, parms)
	} else {
		url = path
	}

	payload, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer payload.Close()

	timeout := time.Duration(5 * time.Second)
	req, err := http.NewRequest(calltype, url, nil)
	client := http.Client{
		Timeout: timeout,
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return res, err
}

func handleRespond(r *http.Response) (*HdfsData, error) {
	hdata := new(HdfsData)
	data, e := ioutil.ReadAll(r.Body)
	log.Println(string(r.Header.Get("")))
	defer r.Body.Close()
	if e != nil {
		return hdata, e
	}
	e = json.Unmarshal(data, hdata)
	if e != nil {
		return hdata, e
	}
	if hdata.RemoteException.Message != "" {
		return hdata, errors.New(hdata.RemoteException.Message)
	}
	return hdata, nil
}
