package main

import (
	"distributeObject/scalability/dataServer/handler"
	"distributeObject/scalability/dataServer/heartbeat"
	"distributeObject/scalability/dataServer/locate"
	"distributeObject/scalability/dataServer/temp"
	"log"
	"net/http"
	"os"
)

func main() {
	locate.CollectObject()
	go heartbeat.StartHeartbeat()
	go locate.StartLocate()

	http.HandleFunc("/objects/", handler.Handler)
	http.HandleFunc("/temp/", temp.Handler)

	log.Fatal(http.ListenAndServe(os.Getenv("LISTEN_ADDRESS"), nil))
}
