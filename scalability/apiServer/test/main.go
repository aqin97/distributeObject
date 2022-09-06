package main

import (
	"bytes"
	"distributeObject/scalability/apiServer/utils"
	"io"
	"log"
	"net/http"
)

//const str = `this object will have only 1 instance`

func main() {
	http.HandleFunc("/test/", testhandler)

	log.Println(http.ListenAndServe(":8003", nil))
}

func testhandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	boo(w, r.Body)
}

func boo(w http.ResponseWriter, r io.Reader) {
	var buf bytes.Buffer
	tee := io.TeeReader(r, &buf)
	hash := utils.CalculateHash(tee)
	w.Write([]byte(hash))
}
