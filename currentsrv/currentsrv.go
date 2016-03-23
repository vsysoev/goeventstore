package main

//DONE:60 Cleanup deadcode after refactoring
//DONE:40 Need one handler to support global state update. Implemented global ScalarState update single database readings
//TODO:20 State may be requested by id or time
//DONE:50 When you connect you get full state and next only updates until reconnect
//DONE:100 Updates of state should be passed through pub/sub
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
	//ScalarState holds global current state
	ScalarState struct {
		state     map[int]map[int]*bson.M
		mx        *sync.Mutex
		isCurrent bool
		lastID    string
	}
	// ClientSlice define type for store clients connected
	ClientSlice []*wsock.Client
)

var (
	sState  ScalarState
	clients ClientSlice
)

func (s ScalarState) serialize2Slice(id string) ([]*bson.M, string, error) {
	var (
		err   error
		nID   uint64
		cID   string
		maxID uint64
		mID   string
		list  []*bson.M
	)
	if id != "" {
		nID, err = strconv.ParseUint(id[len(id)-8:], 16, 32)
		if err != nil {
			return nil, "", err
		}
	}
	s.mx.Lock()
	defer s.mx.Unlock()
	for _, box := range s.state {
		for _, val := range box {
			cID = (*val)["_id"].(bson.ObjectId).Hex()
			curID, err := strconv.ParseUint(cID[len(cID)-8:], 16, 32)
			if err != nil {
				return nil, "", err
			}
			if curID > nID {
				list = append(list, val)
			}
			if maxID < curID {
				maxID = curID
				mID = cID
			}
		}
	}
	return list, mID, nil
}

func messageHandler(ctx context.Context, msgs []interface{}) {
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
			if !sState.isCurrent {
				if sState.lastID == v.(bson.M)["_id"].(bson.ObjectId).Hex() {
					sState.isCurrent = true
				}
			} else {
				sState.lastID = v.(bson.M)["_id"].(bson.ObjectId).Hex()
			}
			ctx.Value("stateUpdateChannel").(chan *bson.M) <- &(vV)
		}
	}
	if sState.isCurrent {
		fmt.Print("+")
	} else {
		fmt.Print("-")
	}
}

func processClientConnection(ctx context.Context, s *wsock.Server) {
	var clients ClientSlice
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
			log.Println("processClientConnection got add client notification", cli)
			//			go clientProcessor(cli)
			clients = append(clients, cli)
			_, toWS, _ := cli.GetChannels()
			if sState.isCurrent {
				m := wsock.MessageT{}
				l, _, err := sState.serialize2Slice("")
				if err != nil {
					log.Println("Error serializing message", err)
				}
				m["state"] = l
				if len(m["state"].([]*bson.M)) > 0 {
					toWS <- &m
				}
			}
			break
		case cli := <-delCh:
			log.Println("delCh got client", cli)
			for i, v := range clients {
				if v == cli {
					clients = append(clients[:i], clients[i+1:]...)
					log.Println("Removed client", cli)
				}
			}
			break
		case msg := <-ctx.Value("stateUpdateChannel").(chan *bson.M):
			out := wsock.MessageT{}
			out["state"] = msg
			for _, v := range clients {
				_, toWS, _ := v.GetChannels()
				toWS <- &out
			}
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
	wsServer := wsock.NewServer(props["current.uri"])
	if wsServer == nil {
		log.Fatalln("Error creating new websocket server")
	}
	sState = ScalarState{}
	sState.isCurrent = false
	sState.state = make(map[int]map[int]*bson.M)
	sState.mx = new(sync.Mutex)
	stateUpdateChannel := make(chan *bson.M, 256)
	err = evStore.Listenner2().Subscribe2("scalar", messageHandler)
	if err != nil {
		log.Fatalln("Error subscribing for changes", err)
	}
	ctx1, cancel := context.WithCancel(context.Background())
	ctx := context.WithValue(ctx1, "stateUpdateChannel", stateUpdateChannel)
	defer cancel()
	sState.lastID = evStore.Listenner2().GetLastId()
	go evStore.Listenner2().Listen(ctx, id)

	go processClientConnection(ctx, wsServer)
	go wsServer.Listen()

	//http.Handle(props["static.url"], http.FileServer(http.Dir("webroot")))
	err = http.ListenAndServe(props["current.url"], nil)
	evStore.Close()
}
