package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"reflect"
	"strconv"
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
	RPCParameter struct {
		params map[string]interface{}
	}
	RPCParameterInterface interface {
		AsString(string) (string, error)
		AsInt64(string) (int64, error)
		AsTimestamp(string) (time.Time, error)
		Serialize() (string, error)
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

func prepareRequest(tag string, filter string) (map[string]interface{}, error) {
	requestParameter := make(map[string]interface{})
	if filter != "" {
		err := json.Unmarshal([]byte(filter), &requestParameter)
		if err != nil {
			return nil, err
		}
	}
	if tag != "" {
		requestParameter["tag"] = make(map[string]interface{})
		requestParameter["tag"].(map[string]interface{})["$eq"] = tag
	}
	return requestParameter, nil
}

func NewRPCParameterInterface(params map[string]interface{}) RPCParameterInterface {
	return &RPCParameter{params}
}

func (p *RPCParameter) AsString(name string) (string, error) {
	if val, ok := p.params[name]; ok {
		switch val.(type) {
		default:
			return "", errors.New("Can't convert " + reflect.TypeOf(val).String() + " to string")
		case string:
			s := val.(string)
			return s, nil
		case int:
			s := strconv.Itoa(val.(int))
			return s, nil
		case float64:
			s := strconv.FormatFloat(val.(float64), 'f', -1, 64)
			return s, nil
		case time.Time:
			s := val.(time.Time).String()
			return s, nil
		}
	}
	return "", errors.New("No parameter with " + name + " found.")
}
func (p *RPCParameter) AsInt64(name string) (int64, error) {
	if val, ok := p.params[name]; ok {
		switch val.(type) {
		default:
			return 0, errors.New("Can't convert " + reflect.TypeOf(val).String() + " to int64")
		case string:
			i, err := strconv.ParseInt(val.(string), 10, 64)
			return i, err
		case int:
			i := val.(int)
			return int64(i), nil
		case int64:
			i := val.(int64)
			return i, nil
		case float64:
			i := int64(val.(float64))
			return i, nil
		case time.Time:
			i := val.(time.Time).Unix()
			return i, nil
		}
	}
	return 0, errors.New("No parameter with " + name + " found.")
}
func (p *RPCParameter) AsTimestamp(name string) (time.Time, error) {
	if val, ok := p.params[name]; ok {
		switch val.(type) {
		default:
			return time.Unix(0, 0), errors.New("Can't convert " + reflect.TypeOf(val).String() + " to time.Time")
		case string:
			t, err := time.Parse(time.RFC3339Nano, val.(string))
			return t, err
		case int64:
			t := time.Unix(val.(int64), 0)
			return t, nil
		case float64:
			t := time.Unix(int64(val.(float64)), 0)
			return t, nil
		case time.Time:
			t := val.(time.Time)
			return t, nil
		}
	}
	return time.Unix(0, 0), errors.New("No parameter with " + name + " found.")
}

func (p *RPCParameter) Serialize() (string, error) {
	b, err := json.Marshal(p.params)
	return string(b), err
}
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
func (f *RPCFunction) GetLastEvent(params RPCParameterInterface) (interface{}, error) {
	if f.evStore == nil {
		return nil, errors.New("EventStore isn't connected")
	}
	sortOrder := "-$natural"
	tag, err := params.AsString("tag")
	if err != nil {
		return nil, err
	}
	filter, err := params.AsString("filter")
	if err != nil {
		return nil, err
	}

	requestParameter, err := prepareRequest(tag, filter)
	if err != nil {
		return nil, err
	}
	ch, err := f.evStore.Query().FindOne(requestParameter, sortOrder)
	return ch, err
}

func (f *RPCFunction) GetHistory(params RPCParameterInterface) (interface{}, error) {
	if f.evStore == nil {
		return nil, errors.New("EventStore isn't connected")
	}
	sortOrder := "$natural"
	tag, err := params.AsString("tag")
	if err != nil {
		return nil, err
	}
	filter, err := params.AsString("filter")
	if err != nil {
		return nil, err
	}
	from, err := params.AsTimestamp("from")
	if err != nil {
		return nil, err
	}
	to, err := params.AsTimestamp("to")
	if err != nil {
		return nil, err
	}
	requestParameter, err := prepareRequest(tag, filter)
	if err != nil {
		return nil, err
	}
	requestParameter["timestamp"] = make(map[string]interface{})
	requestParameter["timestamp"].(map[string]interface{})["$gt"] = from
	requestParameter["timestamp"].(map[string]interface{})["$lt"] = to
	log.Println(requestParameter)
	ch, err := f.evStore.Query().Find(requestParameter, sortOrder)
	return ch, err

}

func (f *RPCFunction) GetDistanceValue(params RPCParameterInterface) (interface{}, error) {
	if f.evStore == nil {
		return nil, errors.New("EventStore isn't connected")
	}
	sortOrder := "-$natural"
	tag, err := params.AsString("tag")
	if err != nil {
		return nil, err
	}
	filter, err := params.AsString("filter")
	if err != nil {
		return nil, err
	}
	from, err := params.AsTimestamp("from")
	if err != nil {
		return nil, err
	}
	to, err := params.AsTimestamp("to")
	if err != nil {
		return nil, err
	}
	if !from.Before(to) {
		return nil, errors.New("To must be greater than From")
	}
	numberOfPoints, err := params.AsInt64("numberOfPoints")
	if err != nil {
		return nil, err
	}
	if numberOfPoints <= 0 {
		return nil, errors.New("numberOfPoints should be positive integer")
	}
	requestParameter, err := prepareRequest(tag, filter)
	if err != nil {
		return nil, err
	}
	requestParameter["timestamp"] = make(map[string]interface{})
	requestParameter["timestamp"].(map[string]interface{})["$lt"] = to
	nanosecondsInOneInterval := to.Sub(from) / time.Duration(numberOfPoints)
	currentPoint := to
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
			e := json.Unmarshal([]byte(s), &m)
			if e != nil {
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
			tmpOut, e := json.Marshal(&points[n])
			if e != nil {
				break
			}
			ch_out <- string(tmpOut)
		}
		close(ch_out)
	}()
	return ch_out, err
}

func (f *RPCFunction) GetFirstEvent(params RPCParameterInterface) (interface{}, error) {
	if f.evStore == nil {
		return nil, errors.New("EventStore isn't connected")
	}
	sortOrder := "$natural"
	tag, err := params.AsString("tag")
	if err != nil {
		return nil, err
	}
	filter, err := params.AsString("filter")
	if err != nil {
		return nil, err
	}
	requestParameter, err := prepareRequest(tag, filter)
	if err != nil {
		return nil, err
	}
	ch, err := f.evStore.Query().FindOne(requestParameter, sortOrder)
	return ch, err
}

func (f *RPCFunction) GetEventAt(tag string, tPoint time.Time, filter string) (chan string, error) {
	if f.evStore == nil {
		return nil, errors.New("EventStore isn't connected")
	}
	sortOrder := "-$natural"
	requestParameter, err := prepareRequest(tag, filter)
	if err != nil {
		return nil, err
	}
	requestParameter["timestamp"] = make(map[string]interface{})
	requestParameter["timestamp"].(map[string]interface{})["$lte"] = tPoint
	log.Println(requestParameter)
	ch, err := f.evStore.Query().FindOne(requestParameter, sortOrder)
	return ch, err
}

func (f *RPCFunction) ListDatabases(params RPCParameterInterface) ([]string, error) {
	return f.evStore.Manager().DatabaseNames()
}

func (f *RPCFunction) Echo(params RPCParameterInterface) (interface{}, error) {
	ch := make(chan string, 1)
	go func() {
		s, err := params.Serialize()
		if err != nil {
			ch <- err.Error()
		} else {
			ch <- s
		}
		close(ch)
	}()
	return ch, nil
}

func clientHandler(ctx context.Context, c wsock.Connector, f *RPCFunction) {
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
		case <-ctx.Done():
			log.Println("Context Done")
			break Loop
		case request, ok := <-fromWS:
			if !ok {
				break Loop
			}
			log.Println("Request", (*request)["method"])

			method := (*request)["method"].(string)
			params := NewRPCParameterInterface((*request)["params"].(map[string]interface{}))
			log.Println("type of params", reflect.TypeOf(params))
			m := reflect.ValueOf(f).MethodByName(method)
			if !m.IsValid() {
				js := wsock.MessageT{}
				js["error"] = "ERROR: No method found. " + method
				toWS <- &js
				return
			}
			mInterface := m.Interface()
			mm := mInterface.(func(RPCParameterInterface) (interface{}, error))
			log.Println(params)
			ch, err := mm(params)
			if err != nil {
				log.Println("Error calling method. ", err)
				return
			}
			log.Println("Function returned", ch)
			js := wsock.MessageT{}
			js["jsonrpc"] = (*request)["jsonrpc"]
			js["id"] = (*request)["id"]
			switch ch.(type) {
			default:
				js["error"] = make(map[string]interface{}, 1)
				js["error"].(map[string]interface{})["code"] = 500
				js["error"].(map[string]interface{})["message"] = "Type is not allowed. " + reflect.TypeOf(ch).String()
			case []string:
				js["result"] = ch
			case chan string:
				js["result"] = make([]interface{}, 0)
				for m := range ch.(chan string) {
					js["result"] = append(js["result"].([]interface{}), m)
				}
			}
			toWS <- &js
			break
		case <-doneCh:
			break Loop
		}
	}
	log.Println("Client disconnected. Exit goroutine")
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
