package temp

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

func patch(w http.ResponseWriter, r *http.Request) {
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	log.Println(r.URL.EscapedPath())
	log.Println("handler.patch uuid:", uuid)
	tempinfo, err := readFromFile(uuid)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	infoFile := os.Getenv("STORAGE_ROOT") + "/temp/" + uuid
	datFile := infoFile + ".dat"
	file, err := os.OpenFile(datFile, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()

	_, err = io.Copy(file, r.Body)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	info, err := file.Stat()
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	actual := info.Size()
	if actual > tempinfo.Size {
		os.Remove(datFile)
		os.ReadDir(infoFile)
		log.Println("actual size", actual, "exceeds", tempinfo.Size)
		w.WriteHeader(http.StatusInternalServerError)
	}

}

func readFromFile(uuid string) (*tempInfo, error) {
	file, err := os.Open(os.Getenv("STORAGE_ROOT") + "/temp/" + uuid)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)
	var info tempInfo
	json.Unmarshal(b, &info)
	return &info, nil
}
