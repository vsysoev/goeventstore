package main

import (
	"encoding/json"
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
		evStore evstore.Connection
	}
	RPCFunctionInterface interface {
		GetFunction(funcName string) (interface{}, error)
	}
	MessageStruct struct {
		Timestamp time.Time   `json:"timestamp"`
		Tag       string      `json:"tag"`
		Event     interface{} `json:"event"`
	}
	OnePoint struct {
		Timestamp      time.Time `json:"timestamp"`
		NumberOfPoints int       `json:"numofpoints"`
		Min            float64   `json:"min"`
		Max            float64   `json:"max"`
		Avg            float64   `json:"avg"`
	}
)

func NewRPCFunctionInterface(e evstore.Connection) RPCFunctionInterface {
	return &RPCFunction{e}
}

func (f *RPCFunction) GetFunction(funcName string) (interface{}, error) {
	m := reflect.ValueOf(f).MethodByName(funcName)
	if !m.IsValid() {
		return nil, errors.New("No function found " + funcName)
	}
	return m.Interface(), nil
}

// FindLastEvent - returns last event with appropriate type
func (f *RPCFunction) FindLastEvent() (chan string, error) {
	if f.evStore == nil {
		return nil, errors.New("EventStore isn't connected")
	}
	sortOrder := "-$natural"
	ch, err := f.evStore.Query().Find(nil, sortOrder)
	return ch, err
}

func (f *RPCFunction) GetHistory(from time.Time, to time.Time) (chan string, error) {
	if f.evStore == nil {
		return nil, errors.New("EventStore isn't connected")
	}
	sortOrder := "$natural"
	requestParameter := make(map[string]interface{})
	requestParameter["timestamp"] = make(map[string]interface{})
	requestParameter["timestamp"].(map[string]interface{})["$gt"] = from
	requestParameter["timestamp"].(map[string]interface{})["$lt"] = to
	log.Println(requestParameter)
	ch, err := f.evStore.Query().Find(requestParameter, sortOrder)
	return ch, err

}

func (f *RPCFunction) GetDistanceValue(from time.Time, to time.Time, numberOfPoints int) (chan string, error) {
	if f.evStore == nil {
		return nil, errors.New("EventStore isn't connected")
	}
	if numberOfPoints <= 0 {
		return nil, errors.New("numberOfPoints should be positive integer")
	}
	log.Println(to.Before(from))
	if !from.Before(to) {
		return nil, errors.New("To must be greater than From")
	}

	nanosecondsInOneInterval := to.Sub(from) / time.Duration(numberOfPoints)
	currentPoint := to
	sortOrder := "-$natural"
	requestParameter := make(map[string]interface{})
	requestParameter["timestamp"] = make(map[string]interface{})
	requestParameter["timestamp"].(map[string]interface{})["$lt"] = to
	ch, err := f.evStore.Query().Find(requestParameter, sortOrder)
	if err != nil {
		return nil, err
	}
	ch_out := make(chan string, 256)
	currentPoint = currentPoint.Add(-nanosecondsInOneInterval)
	points := make([]OnePoint, numberOfPoints)
	currentPointIndex := numberOfPoints - 1
	go func() {
		for {
			s := <-ch
			if s == "" {
				break
			}
			m := MessageStruct{}
			err := json.Unmarshal([]byte(s), &m)
			if err != nil {
				break
			}
			for {
				processSameMessage := false
				if m.Tag == "scalar" {
					if currentPoint.Before(m.Timestamp) {
						if points[currentPointIndex].Min > m.Event.(map[string]interface{})["value"].(float64) || points[currentPointIndex].NumberOfPoints == 0 {
							points[currentPointIndex].Min = m.Event.(map[string]interface{})["value"].(float64)
						}
						if points[currentPointIndex].Max < m.Event.(map[string]interface{})["value"].(float64) || points[currentPointIndex].NumberOfPoints == 0 {
							points[currentPointIndex].Max = m.Event.(map[string]interface{})["value"].(float64)
						}
						points[currentPointIndex].Avg = points[currentPointIndex].Avg + m.Event.(map[string]interface{})["value"].(float64)
						points[currentPointIndex].NumberOfPoints = points[currentPointIndex].NumberOfPoints + 1
					} else {
						points[currentPointIndex].Avg = points[currentPointIndex].Avg / float64(points[currentPointIndex].NumberOfPoints)
						points[currentPointIndex].Timestamp = currentPoint
						currentPoint = currentPoint.Add(-nanosecondsInOneInterval)
						currentPointIndex = currentPointIndex - 1
						processSameMessage = true
					}
				}
				if !processSameMessage {
					break
				}
			}
		}
		points[currentPointIndex].Timestamp = currentPoint
		points[currentPointIndex].Avg = points[currentPointIndex].Avg / float64(points[currentPointIndex].NumberOfPoints)
		for n := range points {
			//			log.Println("Dump points ", points[n])
			tmpOut, err := json.Marshal(&points[n])
			if err != nil {
				break
			}
			ch_out <- string(tmpOut)
		}
		close(ch_out)
	}()
	return ch_out, err
}

func (f *RPCFunction) GetFirstEvent(tag string) (chan string, error) {
	if f.evStore == nil {
		return nil, errors.New("EventStore isn't connected")
	}
	sortOrder := "$natural"
	requestParameter := make(map[string]interface{})
	requestParameter["tag"] = make(map[string]interface{})
	requestParameter["tag"].(map[string]interface{})["$eq"] = tag
	ch, err := f.evStore.Query().FindOne(requestParameter, sortOrder)
	return ch, err
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
