package heartbeat

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aqin97/rabbitmq"
)

var dataServers = make(map[string]time.Time)
var mutex sync.Mutex

func removeExpiredDataServer() {
	for {
		time.Sleep(5 * time.Second)

		mutex.Lock()
		for s, t := range dataServers {
			if t.Add(10 * time.Second).Before(time.Now()) {
				delete(dataServers, s)
			}
		}
		mutex.Unlock()
	}
}

func ListenHeartbeat() {
	q := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	defer q.Close()

	q.Bind("apiServers")

	msgs := q.Consume()
	go removeExpiredDataServer()

	for msg := range msgs {
		dataServer, err := strconv.Unquote(string(msg.Body))
		if err != nil {
			panic(err)
		}
		mutex.Lock()
		dataServers[dataServer] = time.Now()
		mutex.Unlock()
	}

}

func GetDataServer() []string {
	mutex.Lock()
	defer mutex.Unlock()

	dataserver := make([]string, 0)
	for s := range dataServers {
		dataserver = append(dataserver, s)
	}

	return dataserver

}

/*
func ChooseRandomDataServer() string {
	ds := GetDataServer()
	log.Println("heartbeat.ChooseRandomDataServer: dataserver listening port list", ds)
	n := len(ds)
	if n == 0 {
		return ""
	}
	return ds[rand.Intn(n)]
}
*/
func ChooseRandomDataServers(n int, exclude map[int]string) (ds []string) {
	candidates := make([]string, 0)
	reverseExcludeMap := make(map[string]int)
	for id, addr := range exclude {
		reverseExcludeMap[addr] = id
	}
	serrvers := GetDataServer()
	for i := range serrvers {
		s := serrvers[i]
		_, excluded := reverseExcludeMap[s]
		if !excluded {
			candidates = append(candidates, s)
		}
	}
	length := len(candidates)
	if length < n {
		return
	}
	p := rand.Perm(length)
	for i := 0; i < n; i++ {
		ds = append(ds, candidates[p[i]])
	}
	return
}
