package handler

import (
	"distributeObject/scalability/dataServer/locate"
	"distributeObject/scalability/dataServer/utils"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
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

func getFile(hash string) string {
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
