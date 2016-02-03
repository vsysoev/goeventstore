package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

const (
	timeout = time.Millisecond * 10
)

func clientProcessor(c *wsock.Client, evStore *evstore.Connection) {
	var (
		evCh chan string
		err  error
	)
	//	state := make(ScalarState)
	fmt.Println("clientProcessor Client connected. ", c)
	fromWS, toWS, doneCh := c.GetChannels()
	fmt.Print("Before evStore.Subscribe")
	log.Println("Enter main loop serving client")
Loop:
	for {
		select {
		case <-doneCh:
			log.Println("Client disconnected. Exit goroutine")
			evStore.Close()
			//doneCh <- true
			break Loop
		case msg := <-fromWS:
			log.Println("Message recieved in currentsrv", msg)
			evCh, err = evStore.Listenner().Subscribe("")
			if err != nil {
				log.Println("Can't subscribe to evStore", err)
				return
			}
			break
		case msg := <-evCh:
			log.Println("Msg received", msg)
			js := wsock.MessageT{}
			err := json.Unmarshal([]byte(msg), &js)
			if err != nil {
				log.Print("Error event unmarshaling to JSON.", msg)
			}
			toWS <- &js
			break
		case <-time.After(timeout):
			fmt.Print(".")
			break
		}
	}
	log.Println("Exit clientProcessor")
}
func processClientConnection(s *wsock.Server, evStore *evstore.Connection) {
	log.Println("Enter processClientConnection")
	addCh, delCh, doneCh, _ := s.GetChannels()
	log.Println("Get server channels", addCh, delCh, doneCh)
Loop:
	for {
		select {
		case cli := <-addCh:
			log.Println("processClientConnection got add client notification", cli)
			go clientProcessor(cli, evStore)
			break
		case cli := <-delCh:
			log.Println("delCh go client", cli)
			break
		case <-doneCh:
			log.Println("doneCh got message")
			break Loop
		default:
			fmt.Print("*")
			time.Sleep(time.Millisecond * 10)
			break
		}
	}
	log.Println("processClientConnection exited")
}

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
	wsServer := wsock.NewServer(props["websocket.uri"])
	if wsServer == nil {
		log.Fatalln("Error creating new websocket server")
	}
	go processClientConnection(wsServer, evStore)
	go wsServer.Listen()

	//http.Handle(props["static.url"], http.FileServer(http.Dir("webroot")))
	err = http.ListenAndServe(props["websocket.url"], nil)
	evStore.Close()
}
