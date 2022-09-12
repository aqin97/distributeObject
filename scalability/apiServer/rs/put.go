package rs

import (
	"distributeObject/scalability/apiServer/stream"
	"fmt"
	"io"
	"log"

	"github.com/klauspost/reedsolomon"
)

type RSPutStream struct {
	*encoder
}

func NewRSPutStream(dataServers []string, hash string, size int64) (*RSPutStream, error) {
	if len(dataServers) != ALL_SHARDS {
		return nil, fmt.Errorf("dataservers number mismatch")
	}
	//向上取整
	perShard := (size + DATA_SHARDS - 1) / DATA_SHARDS
	writers := make([]io.Writer, ALL_SHARDS)
	var err error
	for i := range writers {
		writers[i], err = stream.NewTempPutStream(dataServers[i], fmt.Sprintf("%s.%d", hash, i), perShard)
		if err != nil {
			return nil, err
		}
	}
	enc := NewEncoder(writers)
	return &RSPutStream{encoder: enc}, nil
}

type encoder struct {
	writers []io.Writer
	enc     reedsolomon.Encoder
	cache   []byte
}

func NewEncoder(writers []io.Writer) *encoder {
	enc, err := reedsolomon.New(DATA_SHARDS, PARITY_SHARDS)
	if err != nil {
		log.Println("rs.NewEncoder err:", err)
	}
	return &encoder{writers, enc, nil}
}

func (e *encoder) Write(p []byte) (n int, err error) {
	length := len(p)
	current := 0
	for length != 0 {
		next := BLOCK_SIZE - len(e.cache)
		if next > 0 {
			next = length
		}
		e.cache = append(e.cache, p[current:current+next]...)
		if len(e.cache) == BLOCK_SIZE {
			e.Flush()
		}
		current += next
		length -= next
	}
	return len(p), nil
}

func (e *encoder) Flush() {
	if len(e.cache) == 0 {
		return
	}
	shards, err := e.enc.Split(e.cache)
	if err != nil {
		log.Println("rs:encoder.Flush err:", err)
	}
	e.enc.Encode(shards)
	for i := range shards {
		e.writers[i].Write(shards[i])
	}
	e.cache = []byte{}
}

func (rs *RSPutStream) Commit(sucess bool) {
	rs.Flush()
	for i := range rs.writers {
		rs.writers[i].(*stream.TempPutStream).Commit(sucess)
	}
}
