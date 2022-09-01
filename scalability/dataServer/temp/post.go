package temp

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type tempInfo struct {
	Uuid string
	Name string
	Size int64
}

func post(w http.ResponseWriter, r *http.Request) {
	output, _ := exec.Command("uuidgen").Output()
	uuid := strings.TrimSuffix(string(output), "\n")
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	size, err := strconv.ParseInt(r.Header.Get("size"), 0, 64)
	if err != nil {
		log.Println("temp.post parse size failed, err:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	t := tempInfo{
		Uuid: uuid,
		Name: name,
		Size: size,
	}
	err = t.writeToFile()
	if err != nil {
		log.Println("temp.post write to file failed, err:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	os.Create(os.Getenv("STORAGE_ROOT") + "/temp/" + t.Uuid + ".dat")
	w.Write([]byte(uuid))
}

func (t *tempInfo) writeToFile() error {
	file, err := os.Create(os.Getenv("STORAGE_ROOT") + "/temp/" + t.Uuid)
	if err != nil {
		return err
	}
	defer file.Close()

	b, _ := json.Marshal(t)
	file.Write(b)
	return nil
}
