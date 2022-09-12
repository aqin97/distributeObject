package temp

import (
	"distributeObject/scalability/dataServer/locate"
	"distributeObject/scalability/dataServer/utils"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type tempInfo struct {
	Uuid string
	Name string
	Size int64
}

func (t *tempInfo) hash() string {
	s := strings.Split(t.Name, ".")
	return s[0]
}

func (t *tempInfo) id() int {
	s := strings.Split(t.Name, ".")
	id, err := strconv.Atoi(s[1])
	if err != nil {
		log.Println("temp.id err:", err)
	}
	return id
}

func commitTempObejct(dataFile string, tempinfo *tempInfo) {
	f, err := os.Open(dataFile)
	if err != nil {
		log.Println("temp.commitTempObejct open file err:", err)
	}
	defer f.Close()

	d := url.PathEscape(utils.CalculateHash(f))
	os.Rename(dataFile, os.Getenv("STORAGE_ROOT")+"/objects/"+tempinfo.Name+"."+d)
	locate.Add(tempinfo.hash(), tempinfo.id())
}

func Handler(w http.ResponseWriter, r *http.Request) {
	m := r.Method
	if m == http.MethodPut {
		put(w, r)
		return
	}
	if m == http.MethodDelete {
		del(w, r)
		return
	}
	if m == http.MethodPost {
		post(w, r)
		return
	}
	if m == http.MethodPatch {
		patch(w, r)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}
