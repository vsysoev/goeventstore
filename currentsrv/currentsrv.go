package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

func main() {
	f, err := os.Create("currentsrv.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		select {
		case <-c:
			fmt.Println("Stop profiling")
			pprof.StopCPUProfile()
			syscall.Exit(0)
		}
	}()

	props := property.Init()
	evStore, err := evstore.Dial(props["mongodb.url"], props["mongodb.db"], props["mongodb.events"])
	if err != nil {
		log.Fatalln("Error connecting to event store. ", err)
	}
	wsServer := wsock.NewServer(props["websocket.uri"], evStore)
	if wsServer == nil {
		log.Fatalln("Error creating new websocket server")
	}
	go wsServer.Listen()

	http.Handle("/", http.FileServer(http.Dir("webroot")))
	err = http.ListenAndServe(props["websocket.url"], nil)
	evStore.Close()
}
