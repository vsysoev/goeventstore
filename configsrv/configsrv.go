package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"gopkg.in/mgo.v2/bson"

	"context"

	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

const (
	timeout = time.Millisecond * 10
)

type (
	// ClientSlice define type for store clients connected
	ClientSlice []*wsock.Client
	// Config struct stores current system configuration
	Config struct {
		config interface{}
		lastID string
		mx     *sync.Mutex
	}
)

var (
	clients       ClientSlice
	currentConfig Config
	isCurrent     bool
)

func configHandler(ctx context.Context, msgs []interface{}) {
	for _, v := range msgs {
		switch v.(bson.M)["tag"] {
		case "config":
			currentConfig.mx.Lock()
			//			s, err := json.Marshal(v.(bson.M))
			//			if err != nil {
			//				log.Println("ERROR reading config from repository.", err.Error())
			//			}
			currentConfig.config = v.(bson.M)
			currentConfig.mx.Unlock()
			if !isCurrent {
				if currentConfig.lastID < v.(bson.M)["_id"].(bson.ObjectId).Hex() {
					isCurrent = true

				}
			} else {
				currentConfig.lastID = v.(bson.M)["_id"].(bson.ObjectId).Hex()
			}
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
				c["msg"] = currentConfig.config
				toWS <- &c
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

	evStore, err := evstore.Dial(props["mongodb.url"], props["mongodb.db"])
	if err != nil {
		log.Fatalln("Error connecting to event store. ", err)
	}
	wsServer := wsock.NewServer(props["configsrv.uri"])
	if wsServer == nil {
		log.Fatalln("Error creating new websocket server")
	}
	currentConfig = Config{}
	currentConfig.mx = &sync.Mutex{}
	isCurrent = false
	stateUpdateChannel := make(chan *bson.M, 256)
	err = evStore.Listenner2(props["configsrv.stream"]).Subscribe2("config", configHandler)
	if err != nil {
		log.Fatalln("Error subscribing for config changes", err)
	}
	ctx1, cancel := context.WithCancel(context.Background())
	ctx := context.WithValue(ctx1, "stateUpdateChannel", stateUpdateChannel)
	defer cancel()
	log.Println("Before Listen call")
	go evStore.Listenner2(props["configsrv.stream"]).Listen(ctx, id)

	go processClientConnection(ctx, wsServer)
	go wsServer.Listen()

	//http.Handle(props["static.url"], http.FileServer(http.Dir("webroot")))
	err = http.ListenAndServe(props["configsrv.url"], nil)
	evStore.Close()
}
