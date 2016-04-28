package main

//DONE:50 Cleanup deadcode after refactoring
//DONE:30 Need one handler to support global state update. Implemented global ScalarState update single database readings
//TODO:10 State may be requested by id or time
//DONE:40 When you connect you get full state and next only updates until reconnect
//DONE:90 Updates of state should be passed through pub/sub
import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
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
		state  map[int]map[int]*bson.M
		mx     *sync.Mutex
		lastID string
	}
	// ClientSlice define type for store clients connected
	ClientSlice []*wsock.Client
	// Config struct stores current system configuration
	Config struct {
		config string
		lastID string
		mx     *sync.Mutex
	}
)

var (
	sState        ScalarState
	clients       ClientSlice
	currentConfig Config
	isCurrent     bool
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

func scalarHandler(ctx context.Context, msgs []interface{}) {
	for _, v := range msgs {
		switch v.(bson.M)["tag"] {
		case "scalar":
			sState.mx.Lock()
			boxID := int(v.(bson.M)["event"].(bson.M)["box_id"].(int))
			varID := int(v.(bson.M)["event"].(bson.M)["var_id"].(int))
			if sState.state[boxID] == nil {
				sState.state[boxID] = make(map[int]*bson.M)
			}
			vV := v.(bson.M)
			sState.state[boxID][varID] = &vV
			sState.mx.Unlock()
			if !isCurrent {
				if sState.lastID == v.(bson.M)["_id"].(bson.ObjectId).Hex() {
					isCurrent = true
				}
			} else {
				sState.lastID = v.(bson.M)["_id"].(bson.ObjectId).Hex()
			}
			ctx.Value("stateUpdateChannel").(chan *bson.M) <- &(vV)
			break
		}
	}
	if isCurrent {
		fmt.Print("+")
	} else {
		fmt.Print("-")
	}
}

func configHandler(ctx context.Context, msgs []interface{}) {
	for _, v := range msgs {
		switch v.(bson.M)["tag"] {
		case "config":
			currentConfig.mx.Lock()
			s, err := json.Marshal(v.(bson.M))
			if err != nil {
				log.Println("ERROR reading config from repository.", err.Error())
			}
			currentConfig.config = string(s)
			currentConfig.mx.Unlock()
			if !isCurrent {
				if currentConfig.lastID == v.(bson.M)["_id"].(bson.ObjectId).Hex() {
					isCurrent = true

				}
			} else {
				currentConfig.lastID = v.(bson.M)["_id"].(bson.ObjectId).Hex()
			}
			log.Println("Configuration changed.", string(s))
			break
		}
	}
	if isCurrent {
		fmt.Print("*")
	} else {
		fmt.Print(".")
	}
}

func processClientConnection(ctx context.Context, s *wsock.Server) {
	var clients ClientSlice
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
			if isCurrent {
				c := wsock.MessageT{}
				c["config"] = currentConfig.config
				toWS <- &c
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
	var id string
	flag.StringVar(&id, "id", "", "ID to subscribe from")
	flag.Parse()
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
	sState.state = make(map[int]map[int]*bson.M)
	sState.mx = &sync.Mutex{}
	currentConfig = Config{}
	currentConfig.mx = &sync.Mutex{}
	isCurrent = false
	stateUpdateChannel := make(chan *bson.M, 256)
	err = evStore.Listenner2().Subscribe2("scalar", scalarHandler)
	if err != nil {
		log.Fatalln("Error subscribing for changes", err)
	}
	err = evStore.Listenner2().Subscribe2("config", configHandler)
	if err != nil {
		log.Fatalln("Error subscribing for config changes", err)
	}
	ctx1, cancel := context.WithCancel(context.Background())
	ctx := context.WithValue(ctx1, "stateUpdateChannel", stateUpdateChannel)
	defer cancel()
	sState.lastID = evStore.Listenner2().GetLastID()
	log.Println("Before Listen call")
	go evStore.Listenner2().Listen(ctx, id)

	go processClientConnection(ctx, wsServer)
	go wsServer.Listen()

	//http.Handle(props["static.url"], http.FileServer(http.Dir("webroot")))
	err = http.ListenAndServe(props["current.url"], nil)
	evStore.Close()
}
