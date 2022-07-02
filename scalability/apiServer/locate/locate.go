package locate

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aqin97/rabbitmq"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	m := r.Method
	if m != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	info := Locate(strings.Split(r.URL.EscapedPath(), "/")[2])
	log.Println(info)
	if len(info) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	b, _ := json.Marshal(info)
	w.Write(b)
}

func Locate(name string) string {
	q := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	q.Publish("dataServers", name)
	msgs := q.Consume()

	go func() {
		time.Sleep(time.Second)
		q.Close()
	}()
	msg := <-msgs
	s, _ := strconv.Unquote(string(msg.Body))
	log.Println("locate.Locate()", s)
	return s
}

func Exist(name string) bool {
	return Locate(name) != ""
}
