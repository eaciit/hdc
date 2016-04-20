package hdfs

import (
	//"encoding/json"
	//"errors"
	"io/ioutil"
	//"os"
	//"fmt"
	"path/filepath"
	"strconv"
)

func (h *WebHdfs) List(path string) (*HdfsData, error) {
	r, e := h.call("GET", path, OP_LISTSTATUS, nil)
	if e != nil {
		return nil, e
	}

	hdata, e := handleRespond(r)
	return hdata, e
}

func (h *WebHdfs) MakeDir(path string, permission string) error {
	if permission == "" {
		permission = "755"
	}
	r, e := h.call("PUT", path, OP_MKDIRS, map[string]string{"permission": permission})
	if e != nil {
		return e
	}
	_, e = handleRespond(r)
	return e
}

func (h *WebHdfs) MakeDirs(paths []string, permission string) map[string]error {
	if permission == "" {
		permission = "755"
	}
	var es map[string]error
	for _, path := range paths {
		e := h.MakeDir(path, permission)
		if e != nil {
			if es == nil {
				es = make(map[string]error, 0)
			}
			es[path] = e
		}
	}
	return es
}

func (h *WebHdfs) PutDir(dirname string, destination string, hostAlias interface{}) (error, map[string]error) {
	fileinfos, err := ioutil.ReadDir(dirname)
	if err != nil {
		return err, nil
	}
	filenames := []string{}
	for _, fi := range fileinfos {
		if fi.IsDir() == false {
			filenames = append(filenames, filepath.Join(dirname, fi.Name()))
		}
	}

	if len(filenames) > 0 {
		es := h.Puts(filenames, destination, "755", nil, hostAlias)
		return nil, es
	}

	return nil, nil
}

func (h *WebHdfs) Rename(path string, destination string) error {
	r, e := h.call("PUT", path, OP_RENAME, map[string]string{"destination": destination})
	if e != nil {
		return e
	}
	_, e = handleRespond(r)
	return e
}

func (h *WebHdfs) deleteObject(path string, recursive bool) error {
	r, e := h.call("DELETE", path, OP_DELETE, map[string]string{"recursive": strconv.FormatBool(recursive)})
	if e != nil {
		return e
	}
	_, e = handleRespond(r)
	return e
}

func (h *WebHdfs) Delete(recursive bool, paths ...string) map[string]error {
	var es map[string]error
	for _, path := range paths {
		e := h.deleteObject(path, recursive)
		if e != nil {
			if es == nil {
				es = map[string]error{}
			}
			es[path] = e
		}
	}
	return es
}
