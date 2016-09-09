package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"context"

	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

// Parameters passed in request:
// id - id of start message
// tag - name of tag to subscribe

const (
	timeout = time.Millisecond * 10
)

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

func clientHandler(ctx context.Context, c *wsock.Client, evStore evstore.Connection) {
	var (
		err error
	)
	//	state := make(ScalarState)
	log.Println("clientProcessor Client connected. ", c.Request())
	id := c.Request().FormValue("id")
	tag := c.Request().FormValue("tag")
	stream := c.Request().FormValue("stream")
	_, toWS, doneCh := c.GetChannels()
	if tag != "" {
		err = evStore.Listenner2(stream).Subscribe2(tag, messageHandler)
		if err != nil {
			log.Println("Can't subscribe to evStore", err)
			return
		}
		ctx2 := context.WithValue(ctx, "toWS", toWS)
		ctx3, cancel := context.WithCancel(ctx2)
		defer cancel()
		go evStore.Listenner2(stream).Listen(ctx3, id)
	} else {
		js := wsock.MessageT{}
		js["response"] = "ERROR: No tag to subscribe"
		toWS <- &js
	}

	log.Println("Enter main loop serving client")
Loop:
	for {
		select {
		case <-doneCh:
			log.Println("Client disconnected. Exit goroutine")
			break Loop
		}
	}
	log.Println("Exit clientProcessor")
}

func processClientConnection(s *wsock.Server, evStore evstore.Connection) {
	log.Println("Enter processClientConnection")
	addCh, delCh, doneCh, _ := s.GetChannels()
	log.Println("Get server channels", addCh, delCh, doneCh)
Loop:
	for {
		select {
		case cli := <-addCh:
			log.Println("processClientConnection got add client notification", cli.Request().FormValue("id"))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go clientHandler(ctx, cli, evStore)
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
			log.Println("Stop profiling")
			pprof.StopCPUProfile()
			syscall.Exit(0)
		}
	}()

	props := property.Init()
	evStore, err := evstore.Dial(props["mongodb.url"], props["mongodb.db"])
	if err != nil {
		log.Fatalln("Error connecting to event store. ", err)
	}
	wsServer := wsock.NewServer(props["events.uri"])
	if wsServer == nil {
		log.Fatalln("Error creating new websocket server")
	}
	go processClientConnection(wsServer, evStore)
	go wsServer.Listen()

	//http.Handle(props["static.url"], http.FileServer(http.Dir("webroot")))
	err = http.ListenAndServe(props["events.url"], nil)
	evStore.Close()
}
