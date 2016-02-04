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
	"github.com/vsysoev/goeventstore/state"
	"github.com/vsysoev/goeventstore/wsock"
)

const (
	timeout = time.Millisecond * 10
)

type (
	ScalarState map[int]map[int]*wsock.MessageT
)

func clientProcessor(c *wsock.Client, evStore *evstore.Connection) {
	var (
		evCh   chan string
		err    error
		sState ScalarState
	)
	sState = make(ScalarState)
	fromWS, toWS, doneCh := c.GetChannels()
	log.Println("Enter main loop serving client")
Loop:
	for {
		select {
		case <-doneCh:
			log.Println("Client disconnected. Exit goroutine")
			evStore.Listenner().Unsubscribe(evCh)
			//doneCh <- true
			break Loop
		case <-fromWS:
			evCh, err = evStore.Listenner().Subscribe("")
			if err != nil {
				log.Println("Can't subscribe to evStore", err)
				return
			}
			break
		case msg := <-evCh:
			js := wsock.MessageT{}
			err := json.Unmarshal([]byte(msg), &js)
			if err != nil {
				log.Print("Error event unmarshaling to JSON.", msg)
			}
			if js["tag"] == "scalar" {
				boxID := int(js["event"].(map[string]interface{})["box_id"].(float64))
				varID := int(js["event"].(map[string]interface{})["var_id"].(float64))
				log.Println("Update state", boxID, varID, js)
				sState[boxID] = make(map[int]*wsock.MessageT)
				sState[boxID][varID] = &js
			}
			break
		case <-time.After(timeout):
			for _, v := range sState {
				for _, oneJS := range v {
					toWS <- oneJS
				}
			}
			sState = make(ScalarState)
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
		}
	}
	log.Println("processClientConnection exited")
}

func currentStateStore(evStore *evstore.Connection, sReader current.StateReader, sUpdater current.StateUpdater) {
	evCh, err := evStore.Listenner().Subscribe("")
	if err != nil {
		return
	}
	defer func() {
		evStore.Listenner().Unsubscribe(evCh)
	}()
	select {
	case msg := <-evCh:
		log.Println(msg)
		break
	case <-time.After(timeout):
		break
	}
	return
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
	stateReader, stateUpdater := current.NewState()
	go currentStateStore(evStore, stateReader, stateUpdater)
	go processClientConnection(wsServer, evStore)
	go wsServer.Listen()

	//http.Handle(props["static.url"], http.FileServer(http.Dir("webroot")))
	err = http.ListenAndServe(props["websocket.url"], nil)
	evStore.Close()
}
