package temp

import (
	"distributeObject/scalability/dataServer/locate"
	"log"
	"net/http"
	"os"
	"strings"
)

func put(w http.ResponseWriter, r *http.Request) {
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	tempinfo, err := readFromFile(uuid)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	infoFile := os.Getenv("STORAGE_ROOT") + "/temp/" + uuid
	datFile := infoFile + ".dat"
	file, err := os.Open(datFile)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	actual := info.Size()
	os.Remove(infoFile)
	if actual != tempinfo.Size {
		os.Remove(datFile)
		log.Println("actual size mismatch, except", tempinfo.Size, "actual", actual)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	commitTempObejct(datFile, tempinfo)
}

func commitTempObejct(dataFile string, tempinfo *tempInfo) {
	os.Rename(dataFile, os.Getenv("STORAGE_ROOT")+"/objects/"+tempinfo.Name)
	locate.Add(tempinfo.Name)
}
