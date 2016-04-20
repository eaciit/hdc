package hdfs

import (
	"errors"
	//"fmt"
	"io/ioutil"
	//"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

func (h *WebHdfs) GetToLocal(path string, destination string, permission string, hostAlias interface{}) error {
	d, err := h.Get(path, hostAlias)
	if err != nil {
		return err
	}
	if permission == "" {
		permission = "755"
	}

	destination = parseHostAlias(destination, hostAlias)

	iperm, _ := strconv.Atoi(permission)
	err = ioutil.WriteFile(destination, d, os.FileMode(iperm))
	if err != nil {
		return err
	}
	return nil
}

func (h *WebHdfs) Get(path string, hostAlias interface{}) ([]byte, error) {
	r, err := h.call("GET", path, OP_OPEN, nil)
	isRedirected := false
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such host") {
			isRedirected = true
		} else {
			return nil, err
		}
	}
	if r != nil {
		if r.StatusCode != 307 {
			return nil, errors.New("Invalid Response Header on OP_OPEN: " + r.Status)
		}
	}

	location := ""
	if isRedirected {
		location = "http:" + strings.Split(err.Error(), ":")[1] + ":" + strings.Split(err.Error(), ":")[2] + ":" + strings.Split(err.Error(), ":")[3]
	} else {
		location = r.Header["Location"][0]
	}

	location = parseHostAlias(location, hostAlias)

	r, err = h.call("GET", location, OP_OPEN, nil)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != 200 {
		return nil, errors.New(r.Status)
	}
	d, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	return d, nil
}

func mergeMapString(source map[string]string, adds map[string]string) map[string]string {
	if source == nil {
		source = make(map[string]string)
	}
	if adds != nil {
		for k, v := range adds {
			source[k] = v
		}
	}
	return source
}

func (h *WebHdfs) Put(localfile string, destination string, permission string, parms map[string]string, hostAlias interface{}) error {
	if permission == "" {
		permission = "755"
	}
	parms = mergeMapString(parms, map[string]string{"permission": permission})
	r, err := h.call("PUT", destination, OP_CREATE, parms)
	if err != nil {
		return err
	}
	if r.StatusCode != 307 {
		return errors.New("Invalid Response Header on OP_CREATE: " + r.Status)
	}

	location := r.Header["Location"][0]
	location = parseHostAlias(location, hostAlias)

	r, err = h.callPayload("PUT", location, OP_CREATE, localfile, nil)
	if err != nil {
		return err
	}
	if r.StatusCode != 201 {
		return errors.New(r.Status)
	}
	return nil
}

func (h *WebHdfs) Puts(paths []string, destinationFolder string, permission string, parms map[string]string, hostAlias interface{}) map[string]error {
	var es map[string]error
	if permission == "" {
		permission = "755"
	}

	fileCount := len(paths)

	//parms = mergeMapString(parms, map[string]string{"permission": strconv.Itoa(permission)})
	ipool := 0
	iprocessing := 0
	iread := 0
	files := []string{}
	for _, path := range paths {
		ipool = ipool + 1
		iread = iread + 1
		files = append(files, path)
		if ipool == h.Config.PoolSize || iread == fileCount {
			wg := sync.WaitGroup{}
			wg.Add(ipool)

			for _, f := range files {
				go func(path string, swg *sync.WaitGroup) {
					defer swg.Done()
					iprocessing = iprocessing + 1
					_, filename := filepath.Split(path)
					newfilename := filepath.Join(destinationFolder, filename)
					e := h.Put(path, newfilename, permission, parms, hostAlias)
					//var e error
					if e != nil {
						if es == nil {
							es = make(map[string]error)
							es[path] = e
						}
						//fmt.Println(path, "=> ", newfilename, " ... FAIL => ", e.Error(), " | Processing ", iprocessing, " of ", fileCount)
					} else {
						//fmt.Println(path, "=> ", newfilename, " ... SUCCESS | Processing ", iprocessing, " of ", fileCount)
					}
				}(f, &wg)
			}

			wg.Wait()
			ipool = 0
			files = []string{}
		}
	}

	return es
}

func (h *WebHdfs) Append(localfile string, destination string) error {
	r, err := h.call("POST", destination, OP_APPEND, nil)
	if err != nil {
		return err
	}
	if r.StatusCode != 307 {
		return errors.New("Invalid Response Header on OP_APPEND: " + r.Status)
	}

	location := r.Header["Location"][0]

	r, err = h.callPayload("POST", location, OP_APPEND, localfile, nil)
	if err != nil {
		return err
	}
	if r.StatusCode != 201 {
		return errors.New(r.Status)
	}
	return nil
}

func (h *WebHdfs) SetOwner(path string, owner string, group string) error {
	ownerInfo := map[string]string{}
	if owner != "" {
		ownerInfo["owner"] = owner
	}
	if group != "" {
		ownerInfo["group"] = group
	}
	r, e := h.call("PUT", path, OP_SETOWNER, ownerInfo)
	if e != nil {
		return e
	}
	if r.StatusCode != 200 {
		return errors.New("Invalid Response Header on OP_SETOWNER: " + r.Status)
	}
	return nil
}

func (h *WebHdfs) SetPermission(path string, permission string) error {
	if permission == "" {
		permission = "755"
	}

	parms := map[string]string{}
	parms["permission"] = permission

	r, e := h.call("PUT", path, OP_SETPERMISSION, parms)
	if e != nil {
		return e
	}
	if r.StatusCode != 200 {
		return errors.New("Invalid Response Header on OP_SETPERMISSION: " + r.Status)
	}
	return nil
}

/*
func (h *WebHdfs) CreateNewFile(path, filename, permission string) error {
	if permission == "" {
		permission = "755"
	}

	parms := map[string]string{}
	parms["permission"] = permission

	var fullpath string

	if string(path[len(path)-1]) == "/" {
		fullpath = path + filename
	} else {
		fullpath = path + "/" + filename
	}

	log.Println(fullpath)

	r, e := h.call("PUT", fullpath, OP_CREATE, parms)
	if e != nil {
		return e
	}
	if r.StatusCode != 200 {
		return errors.New("Invalid Response Header on OP_CREATE: " + r.Status)
	}
	return nil
}
*/
