package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"gopkg.in/mgo.v2/bson"

	"golang.org/x/net/context"

	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

const (
	timeout = time.Millisecond * 10
)

type (
	ScalarState map[int]map[int]*bson.M
)

func (s ScalarState) serialize2Slice() []*bson.M {
	list := make([]*bson.M, 1)
	for _, box := range s {
		for _, val := range box {
			list = append(list, val)
		}
	}
	return list
}

func messageHandler(ctx context.Context, msgs []interface{}) {
	var sState ScalarState
	sState = make(ScalarState)
	log.Println("Msgs received")
	toWS := ctx.Value("toWS").(chan *wsock.MessageT)
	limit := 10
	i := limit
	for _, v := range msgs {
		if v.(bson.M)["tag"] == "scalar" {
			boxID := int(v.(bson.M)["event"].(bson.M)["box_id"].(float64))
			varID := int(v.(bson.M)["event"].(bson.M)["var_id"].(float64))
			if sState[boxID] == nil {
				sState[boxID] = make(map[int]*bson.M)
			}
			vV := v.(bson.M)
			sState[boxID][varID] = &vV
		}
		i--
		if i == 0 {
			out := wsock.MessageT{}
			out["state"] = sState.serialize2Slice()
			toWS <- &out
			log.Println("State sent")
			sState = nil
			sState = make(ScalarState)
			i = 10
		}
	}
	out := wsock.MessageT{}
	out["state"] = sState.serialize2Slice()
	log.Println(out)
	toWS <- &out
	log.Println("State sent")
}

func clientProcessor(c *wsock.Client, evStore *evstore.Connection) {
	var (
		evCh chan string
		err  error
	)
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
		case msg := <-fromWS:
			log.Println("Try to subscribe for ", msg)
			if tag, ok := (*msg)["tag"].(string); ok {
				err = evStore.Listenner2().Subscribe2(tag, messageHandler)
				if err != nil {
					log.Println("Can't subscribe to evStore", err)
					return
				}
				ctx := context.WithValue(context.Background(), "toWS", toWS)
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				id := ""
				if id, ok = (*msg)["id"].(string); ok {
				}
				go evStore.Listenner2().Listen(ctx, id)
			} else {
				log.Println("Can't find tag in message", msg)
				js := wsock.MessageT{}
				js["response"] = "ERROR: No tag to subscribe"
				toWS <- &js
			}
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

	evStore, err := evstore.Dial(props["mongodb.url"], props["mongodb.db"], props["mongodb.stream"])
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
