package handler

import (
	"distributeObject/scalability/apiServer/es"
	"distributeObject/scalability/apiServer/heartbeat"
	"distributeObject/scalability/apiServer/locate"
	"distributeObject/scalability/apiServer/stream"
	"distributeObject/scalability/apiServer/utils"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
)

func put(w http.ResponseWriter, r *http.Request) {
	hash := utils.GetHashFromHeader(r.Header)
	if hash == "" {
		log.Println("missing object hash in digest header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	size := utils.GetSizeFromHeader(r.Header)
	c, err := storageObject(r.Body, hash, size)
	if err != nil {
		log.Println("handler.put err:", err)
		w.WriteHeader(c)
		return
	}
	if c != http.StatusOK {
		w.WriteHeader(c)
		return
	}

	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	err = es.AddVersion(name, hash, size)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func storageObject(r io.Reader, hash string, size int64) (int, error) {
	if locate.Exist(url.PathEscape(hash)) {
		return http.StatusOK, nil
	}

	stream, err := putStream(url.PathEscape(hash), size)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	reader := io.TeeReader(r, stream)
	d := utils.CalculateHash(reader)
	if d != hash {
		stream.Commit(false)
		return http.StatusBadRequest, fmt.Errorf("object hash dismatch, calculate=%s, request=%s", d, hash)
	}
	stream.Commit(true)
	return http.StatusOK, nil
}

func putStream(hash string, size int64) (*stream.TempPutStream, error) {
	server := heartbeat.ChooseRandomDataServer()
	if server == "" {
		return nil, fmt.Errorf("cannot find any dataserver")
	}
	return stream.NewTempPutStream(server, hash, size)
}

//3.0banben
/*
func put(w http.ResponseWriter, r *http.Request) {
	// put 2.0 版本
	//r.URL.EscapedPath() 对url进行转义处理
	object := strings.Split(r.URL.EscapedPath(), "/")[2]
	c, err := storageObject(r.Body, object)
	if err != nil {
		log.Println(err)
	}
	w.WriteHeader(c)


	//put 3.0 版本
	hash := utils.GetHashFromHeader(r.Header)
	log.Println("handler.put hash", hash)
	if hash == "" {
		log.Println("missing object hash in digest header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	//url.PathEscape(hash) 对hash进行转义处理
	c, err := storageObject(r.Body, url.PathEscape(hash))
	if err != nil {
		log.Println(err)
		w.WriteHeader(c)
		return
	}
	if c != http.StatusOK {
		w.WriteHeader(c)
		return
	}
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	size := utils.GetSizeFromHeader(r.Header)
	err = es.AddVersion(name, hash, size)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func storageObject(r io.Reader, hash string, size int64) (int, error) {
	if locate.Exist(url.PathEscape(hash)) {
		return http.StatusOK, nil
	}
	stream, err := putStream(url.PathEscape(hash), size)
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

func putStream(hash string, size int64) (*stream.PutStream, error) {
	server := heartbeat.ChooseRandomDataServer()
	if server == "" {
		return nil, fmt.Errorf("cannot find any dataserver")
	}
	return stream.NewPutStream(server, hash, size), nil
}
*/
