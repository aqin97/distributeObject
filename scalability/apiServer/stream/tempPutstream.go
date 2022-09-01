package stream

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type TempPutStream struct {
	Server string
	Uuid   string
}

func NewTempPutStream(server, hash string, size int64) (*TempPutStream, error) {
	request, err := http.NewRequest("POST", "http://47.100.21.38"+server+"/temp/"+hash, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("size", fmt.Sprintf("%d", size))

	cli := http.Client{}
	response, err := cli.Do(request)
	if err != nil {
		return nil, err
	}
	uuid, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return &TempPutStream{server, string(uuid)}, nil
}

func (w *TempPutStream) Write(p []byte) (n int, err error) {
	requset, err := http.NewRequest("PATCH", "http://47.100.21.38"+w.Server+"/temp/"+w.Uuid, strings.NewReader(string(p)))
	if err != nil {
		return 0, err
	}
	client := http.Client{}
	response, err := client.Do(requset)
	if err != nil {
		return 0, err
	}
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("dataserver retrun http code %d", response.StatusCode)
	}
	return len(p), nil
}

func (w *TempPutStream) Commit(good bool) {
	method := "DELETE"
	if good {
		method = "PUT"
	}
	request, err := http.NewRequest(method, "http://47.100.21.38"+w.Server+"/temp/"+w.Uuid, nil)
	if err != nil {
		log.Println("stream.Commit new http request failed, err:", err)
	}
	client := http.Client{}
	_, err = client.Do(request)
	if err != nil {
		log.Println("stream.Commit failed, err:", err)
	}
}
