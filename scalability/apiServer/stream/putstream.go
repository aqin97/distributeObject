package stream

import (
	"fmt"
	"io"
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
		request, _ := http.NewRequest("PUT", "http://"+server+"/objects/"+object, reader)
		client := http.Client{}
		r, e := client.Do(request)
		if e != nil && r.StatusCode != http.StatusOK {
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
	return <-ps.c
}
