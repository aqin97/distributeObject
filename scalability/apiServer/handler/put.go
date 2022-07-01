package handler

import (
	"distributeObject/scalability/apiServer/heartbeat"
	"distributeObject/scalability/apiServer/stream"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

func put(w http.ResponseWriter, r *http.Request) {
	object := strings.Split(r.URL.EscapedPath(), "/")[2]
	c, err := storageObject(r.Body, object)
	if err != nil {
		log.Println(err)
	}
	w.WriteHeader(c)
}

func storageObject(r io.Reader, object string) (int, error) {
	stream, err := putStream(object)
	if err != nil {
		return http.StatusServiceUnavailable, err
	}
	io.Copy(stream, r)
	err = stream.Close()
	if err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

func putStream(object string) (*stream.PutStream, error) {
	server := heartbeat.ChooseRandomDataServer()
	if server == "" {
		return nil, fmt.Errorf("cannot find any dataserver")
	}
	return stream.NewPutStream(server, object), nil
}
