package main

import (
	"log"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"context"

	"github.com/powerman/rpc-codec/jsonrpc2"
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

type (
	RPC struct {
		c evstore.Connection
	}
	NameArg struct {
		Msg string
	}
	MessageArg struct {
		Stream  string
		SeqID   string
		Tag     string
		Payload string
	}
	ClientType struct {
		e evstore.Connection
		c wsock.ClientInterface
		s wsock.ServerInterface
	}
)

func (rpc *RPC) Echo(m NameArg, reply *NameArg) error {
	*reply = m
	return nil
}

func (rpc *RPC) Submit(m MessageArg, reply *bool) error {
	*reply = true
	log.Println("MessageArg is ", m)
	err := rpc.c.Committer(m.Stream).SubmitEvent(m.SeqID, m.Tag, m.Payload)
	if err != nil {
		*reply = false
	}
	return err
}

func messageHandler(ctx context.Context, stream string, msg []interface{}) {
	log.Println("Msgs received")
	toWS := ctx.Value("toWS").(chan *wsock.MessageT)

	js := wsock.MessageT{}
	log.Println("Before unmarshaling")
	js["msgs"] = msg
	log.Println("After unmarshaling", js)
	toWS <- &js
	log.Println("Msg sent")
}

func (client *ClientType) clientHandler(ctx context.Context) {
	var (
		err error
	)
	//	state := make(ScalarState)
	log.Println("clientProcessor Client connected. ", client.c.Request())
	id := client.c.Request().FormValue("id")
	tag := client.c.Request().FormValue("tag")
	stream := client.c.Request().FormValue("stream")
	_, toWS, doneCh := client.c.GetChannels()
	if tag != "" {
		err = client.e.Listenner2().Subscribe2(stream, tag, "", messageHandler)
		if err != nil {
			log.Println("Can't subscribe to evStore", err)
			return
		}
		ctx2 := context.WithValue(ctx, "toWS", toWS)
		ctx3, cancel := context.WithCancel(ctx2)
		defer cancel()
		go client.e.Listenner2().Listen(ctx3, id)
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

func (client *ClientType) processClientConnections() {
	log.Println("Enter processClientConnection")
	addCh, delCh, doneCh, _ := client.s.GetChannels()
	log.Println("Get server channels", addCh, delCh, doneCh)
Loop:
	for {
		select {
		case cli := <-addCh:
			log.Println("processClientConnection got add client notification", (*cli).Request().FormValue("id"))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client.c = *cli
			go client.clientHandler(ctx)
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
	cl := ClientType{}
	props := property.Init()
	cl.e, err = evstore.Dial(props["mongodb.url"], props["mongodb.db"])
	if err != nil {
		log.Fatalln("Error connecting to event store. ", err)
	}
	defer cl.e.Close()
	cl.s = wsock.NewServer(props["events.uri"])
	if cl.s == nil {
		log.Fatalln("Error creating new websocket server")
	}
	go cl.processClientConnections()
	go cl.s.Listen()

	rpc.Register(&RPC{cl.e})
	http.Handle("/rpc", jsonrpc2.HTTPHandler(nil))

	err = http.ListenAndServe(props["events.url"], nil)
}
