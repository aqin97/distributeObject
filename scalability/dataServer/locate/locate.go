package locate

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/aqin97/rabbitmq"
)

var objects = make(map[string]int)
var mutex sync.Mutex

func Locate(hash string) bool {
	mutex.Lock()
	defer mutex.Unlock()
	_, ok := objects[hash]
	return ok
}

func Add(hash string) {
	mutex.Lock()
	defer mutex.Unlock()
	objects[hash] = 1
}

func Del(hash string) {
	mutex.Lock()
	defer mutex.Unlock()
	delete(objects, hash)
}

func StartLocate() {
	q := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	defer q.Close()
	q.Bind("dataServers")
	msgs := q.Consume()
	for msg := range msgs {
		hash, err := strconv.Unquote(string(msg.Body))
		if err != nil {
			panic(err)
		}
		if Locate(hash) {
			q.Send(msg.ReplyTo, os.Getenv("LISTEN_ADDRESS"))
		}
	}
}

//程序启动时扫盘
func CollectObject() {
	files, err := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/*")
	if err != nil {
		log.Println("locate.CollectObject failed, err:", err)
	}
	for i := range files {
		hash := filepath.Base(files[i])
		objects[hash] = 1
	}
}

/* locate3.0
func Locate(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}

func StartLocate() {
	q := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	defer q.Close()
	q.Bind("dataServers")
	msgs := q.Consume()
	for msg := range msgs {
		object, err := strconv.Unquote(string(msg.Body))
		if err != nil {
			panic(err)
		}
		if Locate(os.Getenv("STORAGE_ROOT") + "/objects/" + object) {
			q.Send(msg.ReplyTo, os.Getenv("LISTEN_ADDRESS"))
		}
	}
}
*/
