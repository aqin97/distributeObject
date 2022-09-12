package rs

import (
	"distributeObject/scalability/apiServer/stream"
	"fmt"
	"io"
	"log"

	"github.com/klauspost/reedsolomon"
)

type RSGetStream struct {
	*decoder
}

type decoder struct {
	readers   []io.Reader
	writers   []io.Writer
	enc       reedsolomon.Encoder
	size      int64
	cache     []byte
	cacheSize int
	total     int64
}

func NewDecoder(readers []io.Reader, writers []io.Writer, size int64) *decoder {
	enc, err := reedsolomon.New(DATA_SHARDS, PARITY_SHARDS)
	if err != nil {
		log.Println("rs.NewDecoder err:", err)
	}
	return &decoder{readers, writers, enc, size, nil, 0, 0}
}

func NewRSGetStream(locateInfo map[int]string, dataServers []string, hash string, size int64) (*RSGetStream, error) {
	if len(locateInfo)+len(dataServers) != ALL_SHARDS {
		return nil, fmt.Errorf("dataServers number dismatch")
	}
	readers := make([]io.Reader, ALL_SHARDS)
	for i := 0; i < ALL_SHARDS; i++ {
		server := locateInfo[i]
		if server == "" {
			locateInfo[i] = dataServers[0]
			dataServers = dataServers[1:]
			continue
		}
		reader, err := stream.NewGetStream(server, fmt.Sprintf("%s.%d", hash, i))
		if err != nil {
			log.Println("rs.NewRSGetStream err:", err)
		}
		readers[i] = reader
	}

	writers := make([]io.Writer, ALL_SHARDS)
	perShard := (size + DATA_SHARDS - 1) / DATA_SHARDS
	var err error
	for i := range readers {
		if readers[i] == nil {
			writers[i], err = stream.NewTempPutStream(locateInfo[i], fmt.Sprintf("%s.%d", hash, i), perShard)
			if err != nil {
				return nil, err
			}
		}
	}
	dec := NewDecoder(readers, writers, size)
	return &RSGetStream{dec}, nil
}

func (d *decoder) Read(p []byte) (n int, err error) {
	if d.cacheSize == 0 {
		err := d.getData()
		if err != nil {
			return 0, nil
		}
	}
	length := len(p)
	if d.cacheSize < length {
		length = d.cacheSize
	}
	d.cacheSize -= length
	copy(p, d.cache[:length])
	return length, nil
}

func (d *decoder) getData() error {
	if d.total == d.size {
		return io.EOF
	}
	shards := make([][]byte, ALL_SHARDS)
	repairIDs := make([]int, 0)
	for i := range shards {
		if d.readers[i] == nil {
			repairIDs = append(repairIDs, i)
		} else {
			shards[i] = make([]byte, BLOCK_PER_SHARD)
			n, err := io.ReadFull(d.readers[i], shards[i])
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				shards[i] = nil
			} else if n != BLOCK_PER_SHARD {
				shards[i] = shards[i][:n]
			}
		}
	}
	err := d.enc.Reconstruct(shards)
	if err != nil {
		return err
	}
	for i := range repairIDs {
		id := repairIDs[i]
		d.writers[id].Write(shards[id])
	}

	for i := 0; i < DATA_SHARDS; i++ {
		shardSize := int64(len(shards[i]))
		if d.total+shardSize > d.size {
			shardSize -= d.total + shardSize - d.size
		}
		d.cache = append(d.cache, shards[i][:shardSize]...)
		d.cacheSize += int(shardSize)
		d.total += shardSize
	}

	return nil
}

func (rs *RSGetStream) Close() {
	for i := range rs.writers {
		if rs.writers[i] != nil {
			rs.writers[i].(*stream.TempPutStream).Commit(true)
		}
	}
}
