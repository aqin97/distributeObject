package handler

import (
	"crypto/sha256"
	"distributeObject/scalability/dataServer/locate"
	"encoding/base64"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	m := r.Method
	if m == http.MethodGet {
		get(w, r)
		return
	}
	/*
		if m == http.MethodPut {
			put(w, r)
			return
		}
	*/

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func get(w http.ResponseWriter, r *http.Request) {
	file := getFile(strings.Split(r.URL.EscapedPath(), "/")[2])
	if file == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	sendFile(w, file)
}

func getFile(name string) string {
	/*
		file := os.Getenv("STORAGE_ROOT") + "/objects/" + hash
		f, _ := os.Open(file)
		d := url.PathEscape(utils.CalculateHash(f))
		f.Close()
		if d != hash {
			log.Println("object hash mismatch, remove", file)
			locate.Del(hash)
			os.Remove(file)
			return ""
		}
		return file
	*/
	files, err := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/" + name + ".*")
	if err != nil {
		log.Println("handler.getData err:", err)
	}
	if len(files) != 1 {
		return ""
	}
	file := files[0]
	h := sha256.New()
	sendFile(h, file)
	d := url.PathEscape(base64.StdEncoding.EncodeToString(h.Sum(nil)))
	hash := strings.Split(file, ".")[2]
	if d != hash {
		log.Println("handler.getData obejct hash dismatch, err:", err)
		locate.Del(hash)
		os.Remove(file)
		return ""
	}
	return file
}

func sendFile(w io.Writer, file string) {
	f, _ := os.Open(file)
	defer f.Close()
	io.Copy(w, f)
}

/*
func get(w http.ResponseWriter, r *http.Request) {
	f, err := os.Open(os.Getenv("STORAGE_ROOT") + "/objects/" + strings.Split(r.URL.EscapedPath(), "/")[2])
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()

	io.Copy(w, f)
}
*/

/*
func put(w http.ResponseWriter, r *http.Request) {
	f, err := os.Create(os.Getenv("STORAGE_ROOT") + "/objects/" + strings.Split(r.URL.EscapedPath(), "/")[2])
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()

	io.Copy(f, r.Body)
}
*/
