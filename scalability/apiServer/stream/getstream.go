package stream

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

type GetSream struct {
	reader io.Reader
}

func newGetStream(url string) (*GetSream, error) {
	r, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dataServer return http code %d", r.StatusCode)
	}
	return &GetSream{r.Body}, nil
}

func NewGetStream(server, object string) (*GetSream, error) {
	if server == "" || object == "" {
		return nil, fmt.Errorf("invalid server %s object %s", server, object)
	}
	url := "http://47.100.21.38" + server + "/objects/" + object
	log.Println(url)
	return newGetStream(url)
}

func (r *GetSream) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}
