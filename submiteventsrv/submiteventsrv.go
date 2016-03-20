package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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
	var ev map[string]interface{}
	fromWS, _, doneCh := c.GetChannels()
Loop:
	for {
		select {
		case <-ctx.Done():
		case <-doneCh:
			break Loop
		case msg := <-fromWS:
			seqid := ""
			if val, ok := (*msg)["sequenceid"].(string); ok {
				seqid = val
			}
			tag := ""
			if val, ok := (*msg)["tag"].(string); ok {
				tag = val
			}
			if val, ok := (*msg)["event"].(map[string]interface{}); ok {
				ev = val
			}
			log.Println(seqid, tag, ev)
			e.Committer().SubmitMapStringEvent(seqid, tag, ev)
			break
		case <-time.After(time.Millisecond * 10):
			break
		}
	}
}

// TODO:0 Events might be submitted through REST interface
func processClientConnection(s *wsock.Server, props property.PropSet) {
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
			streamName := cli.Conn().Request().FormValue("stream")
			if streamName == "" {
				streamName = props["mongodb.stream"]
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			log.Println("Stream name", streamName)
			evStore, err := evstore.Dial(props["mongodb.url"], props["mongodb.db"], streamName)
			defer evStore.Close()
			if err != nil {
				log.Fatalln("Error connecting to event store. ", err)
				return
			}
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
	go func() {
		select {
		case <-c:
			syscall.Exit(0)
		}
	}()
	props := property.Init()
	//DOING:0 evstore should be connected when user connected. Because in request should be defined stream to submit events.
	wsServer := wsock.NewServer(props["submitevents.uri"])
	if wsServer == nil {
		log.Fatalln("Error creating new websocket server")
	}
	go processClientConnection(wsServer, props)
	go wsServer.Listen()

	//http.Handle(props["static.url"], http.FileServer(http.Dir("webroot")))
	err := http.ListenAndServe(props["submitevents.url"], nil)
	if err != nil {
		log.Fatalln("Error while ListenAndServer", err)
	}
}
