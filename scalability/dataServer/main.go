package main

import (
	"distributeObject/scalability/dataServer/handler"
	"distributeObject/scalability/dataServer/heartbeat"
	"distributeObject/scalability/dataServer/locate"
	"log"
	"net/http"
	"os"
)

func main() {
	go heartbeat.StartHeartbeat()
	go locate.StartLocate()

	http.HandleFunc("/objects/", handler.Handler)

	log.Fatal(http.ListenAndServe(os.Getenv("LISTEN_ADDRESS"), nil))
}
