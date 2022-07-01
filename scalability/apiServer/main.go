package main

import (
	"distributeObject/scalability/apiServer/handler"
	"distributeObject/scalability/apiServer/heartbeat"
	"distributeObject/scalability/apiServer/locate"
	"log"
	"net/http"
	"os"
)

func main() {
	go heartbeat.ListenHeartbeat()

	http.HandleFunc("/objects/", handler.Handler)
	http.HandleFunc("/locate/", locate.Handler)

	log.Fatal(http.ListenAndServe(os.Getenv("LISTEN_ADDRESS"), nil))
}
