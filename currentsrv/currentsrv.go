package main

//TODO: clear filter as empty JSON object
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

	"github.com/docker/docker/pkg/pubsub"
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

	// ClientWithFilter stores pointer to client and params
	ClientWithFilter struct {
		client        *wsock.Client
		filter        map[int]bool
		updateChannel chan interface{}
	}
	// ClientSlice define type for store clients connected
	ClientSlice []*ClientWithFilter
)

var (
	sState    ScalarState
	clients   ClientSlice
	isCurrent bool
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
	var (
		boxID, varID int
	)
	for _, v := range msgs {
		switch v.(bson.M)["tag"] {
		case "scalar":
			sState.mx.Lock()
			switch bID := v.(bson.M)["event"].(bson.M)["box_id"].(type) {
			case int:
				boxID = int(bID)
			case int32:
				boxID = int(bID)
			case int64:
				boxID = int(bID)
			case float32:
				boxID = int(bID)
			case float64:
				boxID = int(bID)
			default:
				boxID = -1
				log.Println("Error in boxID", bID)
				return
			}
			switch vID := v.(bson.M)["event"].(bson.M)["var_id"].(type) {
			case int:
				varID = int(vID)
			case int32:
				varID = int(vID)
			case int64:
				varID = int(vID)
			case float32:
				varID = int(vID)
			case float64:
				varID = int(vID)
			default:
				varID = -1
				log.Println("Error in varID", vID)
				return

			}
			if sState.state[boxID] == nil {
				sState.state[boxID] = make(map[int]*bson.M)
			}
			vV := v.(bson.M)
			sState.state[boxID][varID] = &vV
			sState.mx.Unlock()
			if !isCurrent {
				if sState.lastID < v.(bson.M)["_id"].(bson.ObjectId).Hex() {
					isCurrent = true
				}
			} else {
				sState.lastID = v.(bson.M)["_id"].(bson.ObjectId).Hex()
			}
			ctx.Value("stateUpdate").(*pubsub.Publisher).Publish(vV)
			break
		}
	}
	if isCurrent {
		fmt.Print("+")
	} else {
		fmt.Print("-")
	}
}

func handleClient(ctx context.Context) {
	var (
		c ClientWithFilter
	)
	c.client = ctx.Value("client").(*wsock.Client)
	c.filter = make(map[int]bool, 0)
	c.updateChannel = ctx.Value("stateUpdate").(*pubsub.Publisher).Subscribe()
	fromWS, toWS, doneCh := c.client.GetChannels()
Loop:
	for {
		select {
		case <-doneCh:
			log.Println("doneCh in handleClient")
			ctx.Value("stateUpdate").(*pubsub.Publisher).Evict(c.updateChannel)
			break Loop
		case msg := <-fromWS:
			log.Println("Message from WebSocket ", msg)
			fltrArray := make(wsock.MessageT, 1)
			err := json.Unmarshal([]byte(msg.String()), &fltrArray)
			if err != nil {
				log.Println("Error in filter", err)
			} else {
				log.Println("Filter applied", fltrArray)
				if f, ok := fltrArray["filter"]; ok {
					for _, fltr := range f.([]interface{}) {
						if boxID, ok := fltr.(map[string]interface{})["box_id"]; ok {
							if varID, ok := fltr.(map[string]interface{})["var_id"]; ok {
								val := int(boxID.(float64))<<16 + int(varID.(float64))
								c.filter[val] = true
								log.Println(c.filter)
								if isCurrent {
									log.Println(sState.state)
									for boxID, box := range sState.state {
										for varID, val := range box {
											flID := int(boxID)<<16 + int(varID)
											if _, ok := c.filter[flID]; ok {
												m := wsock.MessageT{}
												m["msg"] = val
												toWS <- &m
											}
										}
									}
								}
							} else {
								log.Println("Error not varID in filter")
								c.filter = make(map[int]bool, 0)
							}
						} else {
							log.Println("Error not boxID in filter")
							c.filter = make(map[int]bool, 0)
						}
					}
				}
			}
			break
		case stateMsg := <-c.updateChannel:
			var (
				bID, vID int
				ok       bool
			)
			log.Println(stateMsg)
			bID, ok = ((stateMsg).(bson.M))["event"].(bson.M)["box_id"].(int)
			if !ok {
				bID = int(((stateMsg).(bson.M))["event"].(bson.M)["box_id"].(float64))
			}
			vID, ok = ((stateMsg).(bson.M))["event"].(bson.M)["var_id"].(int)
			if !ok {
				bID = int(((stateMsg).(bson.M))["event"].(bson.M)["var_id"].(float64))
			}
			flID := bID<<16 + vID
			if _, ok := c.filter[flID]; ok {
				m := wsock.MessageT{}
				m["msg"] = stateMsg
				toWS <- &m
			}

			break
		case <-ctx.Done():
			ctx.Value("stateUpdate").(*pubsub.Publisher).Evict(c.updateChannel)
			log.Println("Context closed")
			break Loop
		}
	}
}

func processClientConnection(ctx context.Context, s *wsock.Server) {
	var (
		clients ClientSlice
	)
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
			clientContext := context.WithValue(ctx, "client", cli)
			go handleClient(clientContext)
			break
		case cli := <-delCh:
			log.Println("delCh got client", cli)
			for i, v := range clients {
				if v.client == cli {
					clients = append(clients[:i], clients[i+1:]...)
					log.Println("Removed client", cli)
				}
			}
			break
			/*		case msg := <-ctx.Value("stateUpdateChannel").(chan *bson.M):
					out := wsock.MessageT{}
					out["state"] = msg
					for _, v := range clients {
						_, toWS, _ := v.client.GetChannels()
						toWS <- &out
					}
			*/
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
	isCurrent = false
	stateUpdate := pubsub.NewPublisher(time.Millisecond*100, 1024)
	err = evStore.Listenner2().Subscribe2("scalar", scalarHandler)
	if err != nil {
		log.Fatalln("Error subscribing for changes", err)
	}
	ctx1, cancel := context.WithCancel(context.Background())
	ctx := context.WithValue(ctx1, "stateUpdate", stateUpdate)
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
