package main

//DONE: Need one handler to support global state update. Implemented global ScalarState
//  update single database readings
//TODO: State may be requested by id or time
//TODO: When you connect you get full state and next only updates until reconnect
import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"sync"
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
	ScalarState struct {
		state map[int]map[int]*bson.M
		mx    sync.Mutex
	}
)

var (
	sState ScalarState
)

func (s ScalarState) serialize2Slice(id string) ([]*bson.M, error) {
	var (
		err error
		nId uint64
	)
	if id != "" {
		nId, err = strconv.ParseUint(id[len(id)-8:], 16, 32)
		if err != nil {
			return nil, err
		}
	}
	s.mx.Lock()
	defer s.mx.Unlock()
	list := make([]*bson.M, 1)
	for _, box := range s.state {
		for _, val := range box {
			log.Println("ids", val, nId)
			cId := (*val)["_id"].(bson.ObjectId).Hex()
			curId, err := strconv.ParseUint(cId[len(cId)-8:], 16, 32)
			if err != nil {
				return nil, err
			}
			if curId > nId {
				list = append(list, val)
			}
		}
	}
	return list, nil
}

func messageHandler(ctx context.Context, msgs []interface{}) {
	log.Println("Message recieved")
	for _, v := range msgs {
		if v.(bson.M)["tag"] == "scalar" {
			sState.mx.Lock()
			boxID := int(v.(bson.M)["event"].(bson.M)["box_id"].(int))
			varID := int(v.(bson.M)["event"].(bson.M)["var_id"].(int))
			if sState.state[boxID] == nil {
				sState.state[boxID] = make(map[int]*bson.M)
			}
			vV := v.(bson.M)
			sState.state[boxID][varID] = &vV
			sState.mx.Unlock()
		}
	}
	log.Println(sState)
}

func sendStatusToClient(ctx context.Context, id string) {
	var err error
	_, toWS, doneCh := ctx.Value("client").(*wsock.Client).GetChannels()
	go func() {
		<-doneCh
		ctx.Done()
	}()
	out := wsock.MessageT{}
	for {
		out["state"], err = sState.serialize2Slice(id)
		if err != nil {
			log.Println("Error serializing message", err)
		}

		toWS <- &out
	}
}
func clientProcessor(c *wsock.Client, evStore *evstore.Connection) {
	var (
		id string
		ok bool
	)
	fromWS, _, doneCh := c.GetChannels()
	log.Println("Enter main loop serving client")
Loop:
	for {
		select {
		case <-doneCh:
			log.Println("Client disconnected. Exit goroutine")
			break Loop
		case msg := <-fromWS:
			log.Println("Try to subscribe for ", msg)
			ctx := context.WithValue(context.Background(), "client", c)
			if id, ok = (*msg)["id"].(string); ok {
			} else {
				id = ""
			}
			go sendStatusToClient(ctx, id)
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
	var id string
	flag.StringVar(&id, "id", "", "ID to subscribe from")
	flag.Parse()
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
	sState = ScalarState{}
	sState.state = make(map[int]map[int]*bson.M)
	err = evStore.Listenner2().Subscribe2("scalar", messageHandler)
	if err != nil {
		log.Fatalln("Error subscribing for changes", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go evStore.Listenner2().Listen(ctx, id)

	go processClientConnection(wsServer, evStore)
	go wsServer.Listen()

	//http.Handle(props["static.url"], http.FileServer(http.Dir("webroot")))
	err = http.ListenAndServe(props["websocket.url"], nil)
	evStore.Close()
}
