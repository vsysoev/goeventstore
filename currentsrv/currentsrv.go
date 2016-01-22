package main

import (
	"log"
	"net/http"

	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

func main() {
	props := property.Init()
	evStore, err := evstore.Dial(props["mongodb.url"].(string), props["mongodb.db"].(string), props["mongodb.events"].(string))
	if err != nil {
		log.Fatalln("Error connecting to event store. ", err)
	}
	wsServer := wsock.NewServer(props["websocket.uri"].(string), evStore)
	if wsServer == nil {
		log.Fatalln("Error creating new websocket server")
	}
	go wsServer.Listen()

	http.Handle("/", http.FileServer(http.Dir("webroot")))
	err = http.ListenAndServe(props["websocket.url"].(string), nil)
	evStore.Close()
}
