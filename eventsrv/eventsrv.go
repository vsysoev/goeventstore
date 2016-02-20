package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

const (
	timeout = time.Millisecond * 10
)

// DOING:0 Move to Listenner2 and callback

// TODO:0 Implementes messageHandler
func messageHandler(ctx context.Context, msg []interface{}) {
	log.Println("Msgs received")
	toWS := ctx.Value("toWS").(chan *wsock.MessageT)

	js := wsock.MessageT{}
	log.Println("Before unmarshaling")
	js["msgs"] = msg
	log.Println("After unmarshaling", js)
	toWS <- &js
	log.Println("Msg sent")
}

// TODO:50 should be context.Context used to pass wsock.Client
func clientHandler(c *wsock.Client, evStore *evstore.Connection) {
	var (
		err error
	)
	//	state := make(ScalarState)
	log.Println("clientProcessor Client connected. ", c)
	fromWS, toWS, doneCh := c.GetChannels()
	log.Println("Enter main loop serving client")
Loop:
	for {
		select {
		case <-doneCh:
			log.Println("Client disconnected. Exit goroutine")
			break Loop
		case msg := <-fromWS:
			log.Println("Message recieved from WS", msg)
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

// TODO:30 implement context creation for the server
func processClientConnection(s *wsock.Server, evStore *evstore.Connection) {
	log.Println("Enter processClientConnection")
	addCh, delCh, doneCh, _ := s.GetChannels()
	log.Println("Get server channels", addCh, delCh, doneCh)
Loop:
	for {
		select {
		case cli := <-addCh:
			log.Println("processClientConnection got add client notification", cli)
			go clientHandler(cli, evStore)
			break
		case cli := <-delCh:
			log.Println("delCh go client", cli)
			break
		case <-doneCh:
			log.Println("doneCh got message")
			break Loop
			//		case <-time.After(timeout):
			//			break
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
			log.Println("Stop profiling")
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
