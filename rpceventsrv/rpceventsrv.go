package main

import (
	"errors"
	"log"
	"net/http"
	"reflect"
	"time"

	"golang.org/x/net/context"

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
	// RPCFunction struct to process query
	RPCFunction struct {
		evStore *evstore.Connection
	}
)

// FindLastEvent - returns last event with appropriate type
func (f *RPCFunction) FindLastEvent(params string) (chan string, error) {
	log.Println("FindLastEvent Called")
	if f.evStore == nil {
		return nil, errors.New("EventStore isn't connected")
	}
	//	ch, err := f.evStore.Query().
	//	return ch, err
	return nil, errors.New("Not implmented")
}

func (f *RPCFunction) Echo(params []interface{}) (interface{}, error) {

	return params, nil
}

func clientHandler(ctx context.Context, c *wsock.Client, f *RPCFunction) {
	var (
	//	err error
	)
	defer c.Done()
	//	state := make(ScalarState)
	log.Println("clientProcessor Client connected. ", c.Request())
	fromWS, toWS, doneCh := c.GetChannels()
Loop:
	for {
		log.Println("Reading loop")
		select {
		case request, ok := <-fromWS:
			if !ok {
				break Loop
			}
			log.Println("Request", (*request)["method"])

			method := (*request)["method"].(string)
			params := (*request)["params"]
			log.Println("type of params", reflect.TypeOf(params))
			m := reflect.ValueOf(f).MethodByName(method)
			if !m.IsValid() {
				js := wsock.MessageT{}
				js["error"] = "ERROR: No method found. " + method
				toWS <- &js
				return
			}
			mInterface := m.Interface()
			mm := mInterface.(func([]interface{}) (interface{}, error))
			ch, err := mm(params.([]interface{}))
			if err != nil {
				log.Println("Error calling method. ", err)
				return
			}
			log.Println("Function returned", ch)
			js := wsock.MessageT{}
			js["jsonrpc"] = (*request)["jsonrpc"]
			js["id"] = (*request)["id"]
			js["result"] = ch

			toWS <- &js
			break
		case <-doneCh:
			log.Println("Client disconnected. Exit goroutine")
			break Loop
		}
	}
	return
}

func processClientConnection(s *wsock.Server, f *RPCFunction) {
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
			go clientHandler(ctx, cli, f)
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

	props := property.Init()
	evStore, err := evstore.Dial(props["mongodb.url"], props["mongodb.db"], props["mongodb.stream"])
	if err != nil {
		log.Fatalln("Error connecting to event store. ", err)
	}
	f := RPCFunction{evStore}
	wsServer := wsock.NewServer(props["rpceventsrv.uri"])
	if wsServer == nil {
		log.Fatalln("Error creating new websocket server")
	}
	go processClientConnection(wsServer, &f)
	go wsServer.Listen()

	//http.Handle(props["static.url"], http.FileServer(http.Dir("webroot")))
	err = http.ListenAndServe(props["rpceventsrv.url"], nil)
	evStore.Close()
}
