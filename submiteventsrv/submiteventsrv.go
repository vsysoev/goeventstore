package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"golang.org/x/net/context"

	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

type (
	Connector wsock.Connector
)

// DONE:40 Events might be submitted through websocket
func handleClientRequest(ctx context.Context, c Connector, e *evstore.Connection) {
	fromWS, _, doneCh := c.GetChannels()
Loop:
	for {
		select {
		case <-ctx.Done():
		case <-doneCh:
			break Loop
		case msg := <-fromWS:
			e.Committer().SubmitEvent((*msg)["sequenceid"].(string),
				(*msg)["tag"].(string),
				(*msg)["event"].(string))
			break
		case <-time.After(time.Millisecond * 10):
			break
		}
	}
}

// TODO:0 Events might be submitted through REST interface
func processClientConnection(s *wsock.Server, evStore *evstore.Connection) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log.Println("Enter processClientConnection")
	addCh, delCh, doneCh, _ := s.GetChannels()
	log.Println("Get server channels", addCh, delCh, doneCh)
Loop:
	for {
		select {
		case <-doneCh:
			log.Println("doneCh got message")
			break Loop
		case <-ctx.Done():
			log.Println("Context destroyed")
			break Loop
		case cli := <-addCh:
			log.Println("processClientConnection got add client notification", cli.Request().FormValue("id"))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go handleClientRequest(ctx, cli, evStore)
			break
		case cli := <-delCh:
			log.Println("delCh go client", cli)
			break
		}
	}
	log.Println("processClientConnection exited")
}

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	props := property.Init()
	evStore, err := evstore.Dial(props["mongodb.url"], props["mongodb.db"], props["mongodb.stream"])
	if err != nil {
		log.Fatalln("Error connecting to event store. ", err)
	}
	wsServer := wsock.NewServer(props["submitevents.uri"])
	if wsServer == nil {
		log.Fatalln("Error creating new websocket server")
	}
	go processClientConnection(wsServer, evStore)
	go wsServer.Listen()

	//http.Handle(props["static.url"], http.FileServer(http.Dir("webroot")))
	err = http.ListenAndServe(props["submitevents.url"], nil)
	evStore.Close()
}
