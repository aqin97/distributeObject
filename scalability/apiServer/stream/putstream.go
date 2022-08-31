package stream

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

type PutStream struct {
	writer *io.PipeWriter
	c      chan error
}

func NewPutStream(server, object string) *PutStream {
	reader, writer := io.Pipe()
	c := make(chan error)
	go func() {

		url := "http://47.100.21.38" + server + "/objects/" + object
		log.Println("stream.NewPutStream: url:", url)
		request, err := http.NewRequest("PUT", url, reader)
		if err != nil {
			log.Println("stream.NewPutStream: err1:", err)
		}
		client := http.Client{}
		r, e := client.Do(request)
		if e != nil {
			log.Println("stream.NewPutStream: err2:", e)
		}
		if e == nil && r.StatusCode != http.StatusOK {
			e = fmt.Errorf("dataServer return http code %d", r.StatusCode)
		}
		c <- e
	}()
	return &PutStream{writer, c}
}

func (ps *PutStream) Write(p []byte) (n int, err error) {
	return ps.writer.Write(p)
}

func (ps *PutStream) Close() error {
	ps.writer.Close()
	return <-ps.c
}
