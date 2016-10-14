package main

import (
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"testing"
	"time"

	"context"

	"github.com/vsysoev/goeventstore/evstore"
)

const dbName string = "rpceventsrv_test"

type (
	fakeDB         struct{}
	fakeCommitter  struct{}
	fakeListenner  struct{}
	fakeListenner2 struct{}
	fakeManager    struct{}
	fakeQuery      struct{}
)

func NewFakeDB() (evstore.Connection, error) {
	return &fakeDB{}, nil
}

func NewFailedFakeDB() (evstore.Connection, error) {
	return nil, errors.New("FakeDB Failed to connect")
}
func (f *fakeDB) Committer(stream string) evstore.Committer {
	return &fakeCommitter{}
}
func (f *fakeDB) Listenner2() evstore.Listenner2 {
	return &fakeListenner2{}
}
func (f *fakeDB) Manager() evstore.Manager {
	return &fakeManager{}
}
func (f *fakeDB) Query(stream string) evstore.Query {
	return &fakeQuery{}
}
func (f *fakeDB) Close() {

}
func (f *fakeCommitter) SubmitEvent(sequenceID string, tag string, eventJSON string) error {
	return errors.New("Not implemented")
}
func (f *fakeCommitter) SubmitMapStringEvent(sequenceID string, tag string, body map[string]interface{}) error {
	return errors.New("Not implemented")
}
func (f *fakeCommitter) PrepareStream() error {
	return errors.New("Not implemented")
}

func (f *fakeListenner) Subscribe(fromID string) (chan string, error) {
	return nil, errors.New("Not implemented")
}
func (f *fakeListenner) Unsubscribe(eventChannel chan string) {

}

func (f *fakeListenner2) Subscribe2(stream string, eventTypes string, id string, handlerFunc evstore.Handler) error {
	return errors.New("Not implemented")
}
func (f *fakeListenner2) Unsubscribe2(stream string, eventTypes string) {

}
func (f *fakeListenner2) GetLastID(stream string) string {
	return "Not implemented"
}
func (f *fakeListenner2) Listen(ctx context.Context, id string) error {
	return errors.New("Not implemented")
}

func (f *fakeManager) DropDatabase(databaseName string) error {
	return errors.New("Not implemented")
}

func (f *fakeManager) DatabaseNames() ([]string, error) {
	return nil, errors.New("Not implemented")
}
func (f *fakeManager) CollectionNames() ([]string, error) {
	return nil, errors.New("Not implemented")
}
func (f *fakeQuery) Find(params interface{}, sortOrder string) (chan string, error) {
	return nil, errors.New("Not implemented")
}

func (f *fakeQuery) FindOne(params interface{}, sortOrder string) (chan string, error) {
	return nil, errors.New("Not implemented")
}

func (f *fakeQuery) Pipe(fakePipeline interface{}) (chan string, error) {
	return nil, errors.New("Not implemented")
}

func initEventStore(url string, dbName string) (evstore.Connection, error) {
	evStore, err := evstore.Dial(url, dbName)
	if err != nil {
		return nil, err
	}
	evStore.Manager().DropDatabase(dbName)
	return evStore, err
}

func submitNScalars(evStore evstore.Connection, stream string, number int, boxID int, varID int, delay time.Duration) error {
	for n := 0; n < number; n++ {
		msg := "{\"box_id\":" + strconv.Itoa(boxID) + ", \"var_id\":" + strconv.Itoa(varID) + ", \"value\":" + strconv.FormatFloat(float64(n), 'f', -1, 32) + "}"
		err := evStore.Committer(stream).SubmitEvent("", "scalar", msg)
		if err != nil {
			return err
		}
		if delay > 0 {
			<-time.After(delay)
		}
	}
	return nil
}
func getRPCFunction(evStore evstore.Connection, funcName string) (interface{}, error) {
	rpc := NewRPCFunctionInterface(evStore)
	if rpc == nil {
		return rpc, errors.New("Can't get RPCFuncitonInterface")
	}
	return rpc.GetFunction(funcName)
}

func TestNilEventStore(t *testing.T) {
	var (
		rpc RPCFunction
	)
	log.Println(rpc.evStore)
	if rpc.evStore != nil {
		t.Fatal("Error. evStore initialed but should not")
	}
}

func TestNewRPCFunctionInterface(t *testing.T) {
	c, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal("Error connecting to evstore")
	}
	rpc := NewRPCFunctionInterface(c)
	if err != nil {
		t.Fatal(err)
	}
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
}

func TestNegativeNewRPCFunctionInterface(t *testing.T) {
	evStore, err := NewFailedFakeDB()
	if err == nil {
		t.Fatal("Should be an error")
	}
	rpc := NewRPCFunctionInterface(evStore)
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
}

func TestGetFunction(t *testing.T) {
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "Echo")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
}
func TestNegativeGetFunction(t *testing.T) {
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "EchoFake")
	if err == nil {
		t.Fatal(err)
	}
	if f != nil {
		t.Fatal("Function isn't nil")
	}
}

func TestFailedGetLastEvent(t *testing.T) {
	rpc := NewRPCFunctionInterface(nil)
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
	f, err := rpc.GetFunction("GetLastEvent")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	p := make(map[string]interface{})
	p["tag"] = ""
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	_, err = (f.(func(RPCParameterInterface) (interface{}, error)))(prms)
	if err == nil {
		t.Fatal("Should be an error")
	}
	if err.Error() != "EventStore isn't connected" {
		t.Fatal(err)
	}
}

func TestGetHistory(t *testing.T) {
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetHistory")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	msg1 := "{\"message\":\"NOT expected\"}"
	evStore.Committer("scalar_100").SubmitEvent("", "scalar", msg1)
	<-time.After(1 * time.Second)
	tStart := time.Now()
	<-time.After(1 * time.Second)
	submitNScalars(evStore, "scalar_100", 10, 1, 1, 100*time.Millisecond)
	tStop := time.Now()
	<-time.After(2 * time.Second)
	evStore.Committer("scalar_100").SubmitEvent("", "scalar", msg1)
	log.Println(tStart, " < ", tStop)
	p := make(map[string]interface{})
	p["stream"] = "scalar_100"
	p["tag"] = "scalar"
	p["from"] = tStart
	p["to"] = tStop
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (interface{}, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	expected := 0
	msgCount := 0
	for msg := range c.(chan string) {
		m := make(map[string]interface{}, 1)
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if m["event"].(map[string]interface{})["value"] != float64(expected) {
			t.Fatal(m["event"], "Expected ", expected)
		}
		expected = expected + 1
		msgCount = msgCount + 1
	}
	if msgCount != 10 {
		t.Fatal("Incorrect amount of messages returned.", msgCount)
	}
}

func TestGetHistoryFilter(t *testing.T) {
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	msg1 := "{\"message\":\"NOT expected\"}"
	evStore.Committer("scalar_100").SubmitEvent("", "scalar", msg1)
	<-time.After(1 * time.Second)
	tStart := time.Now()
	<-time.After(1 * time.Second)
	submitNScalars(evStore, "scalar_100", 10, 1, 1, 100*time.Millisecond)
	submitNScalars(evStore, "scalar_100", 20, 1, 2, 100*time.Millisecond)
	tStop := time.Now()
	<-time.After(2 * time.Second)
	evStore.Committer("scalar_100").SubmitEvent("", "scalar", msg1)
	log.Println(tStart, " < ", tStop)
	f, err := getRPCFunction(evStore, "GetHistory")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	p := make(map[string]interface{})
	p["stream"] = "scalar_100"
	p["tag"] = "scalar"
	p["from"] = tStart
	p["to"] = tStop
	p["filter"] = "{\"event.box_id\": { \"$eq\": 1 }, \"event.var_id\": {\"$eq\": 2}}"
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (interface{}, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	expected := 0
	msgCount := 0
	for msg := range c.(chan string) {
		m := make(map[string]interface{}, 1)
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if m["event"].(map[string]interface{})["box_id"] != float64(1) {
			t.Fatal(m["event"], "Expected box_id 1")
		}
		if m["event"].(map[string]interface{})["var_id"] != float64(2) {
			t.Fatal(m["event"], "Expected var_id 2")
		}
		if m["event"].(map[string]interface{})["value"] != float64(expected) {
			t.Fatal(m["event"], "Expected ", expected)
		}
		expected = expected + 1
		msgCount = msgCount + 1
	}
	if msgCount != 20 {
		t.Fatal("Incorrect amount of messages returned.", msgCount)
	}
}

func TestGetDistanceValueIncorrectPointNumber(t *testing.T) {
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetDistanceValue")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	tStart := time.Now()
	tStop := tStart.Add(10 * time.Second)
	p := make(map[string]interface{})
	p["id"] = 1
	p["tag"] = "scalar"
	p["from"] = tStart
	p["to"] = tStop
	p["numberOfPoints"] = -10
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	_, err = f.(func(RPCParameterInterface) (interface{}, error))(prms)
	expectedError := "numberOfPoints should be positive integer"
	if err.Error() != expectedError {
		t.Fatal(err.Error() + " != " + expectedError)
	}

}

func TestGetDistanceValueIncorrectInterval(t *testing.T) {
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetDistanceValue")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	tStop := time.Now()
	tStart := tStop.Add(10 * time.Second)
	p := make(map[string]interface{})
	p["id"] = 1
	p["tag"] = "scalar"
	p["from"] = tStart
	p["to"] = tStop
	p["numberOfPoints"] = 10
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	_, err = f.(func(RPCParameterInterface) (interface{}, error))(prms)
	expectedError := "To must be greater than From"
	if err.Error() != expectedError {
		t.Fatal(err.Error() + " != " + expectedError)
	}

}
func TestGetDistanceValueZerroInterval(t *testing.T) {
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetDistanceValue")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	tStart := time.Now()
	p := make(map[string]interface{})
	p["id"] = 1
	p["tag"] = "scalar"
	p["from"] = tStart
	p["to"] = tStart
	p["numberOfPoints"] = 10
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	_, err = f.(func(RPCParameterInterface) (interface{}, error))(prms)
	expectedError := "To must be greater than From"
	if err.Error() != expectedError {
		t.Fatal(err.Error() + " != " + expectedError)
	}

}

func TestGetDistanceValue(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetDistanceValue")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	msg1 := "{\"message\":\"NOT expected\"}"
	evStore.Committer("test").SubmitEvent("", "test", msg1)
	<-time.After(1 * time.Second)
	tStart := time.Now()
	<-time.After(10 * time.Millisecond)
	submitNScalars(evStore, "test", 100, 1, 1, 100)
	tStop := time.Now()
	<-time.After(2 * time.Second)
	evStore.Committer("scalar").SubmitEvent("", "test", msg1)
	log.Println(tStart, " < ", tStop)
	p := make(map[string]interface{})
	p["id"] = 1
	p["tag"] = "scalar"
	p["from"] = tStart
	p["to"] = tStop
	p["numberOfPoints"] = 10
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (interface{}, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c.(chan string) {
		msgCounter = msgCounter + 1
		//		log.Println(msg)
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
}

func TestGetDistanceValueFilter(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetDistanceValue")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	msg1 := "{\"message\":\"NOT expected\"}"
	evStore.Committer("scalar").SubmitEvent("", "test", msg1)
	<-time.After(1 * time.Second)
	tStart := time.Now()
	<-time.After(10 * time.Millisecond)
	submitNScalars(evStore, "test", 100, 1, 1, 100*time.Millisecond)
	submitNScalars(evStore, "test", 100, 3, 1, 0)
	tStop := time.Now()
	<-time.After(2 * time.Second)
	evStore.Committer("scalar").SubmitEvent("", "test", msg1)
	log.Println(tStart, " < ", tStop)
	p := make(map[string]interface{})
	p["id"] = 1
	p["tag"] = "scalar"
	p["from"] = tStart
	p["to"] = tStop
	p["numberOfPoints"] = 10
	p["filter"] = "{\"event.box_id\": { \"$eq\": 1 }, \"event.var_id\": {\"$eq\": 1}}"
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (interface{}, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c.(chan string) {
		msgCounter = msgCounter + 1
		//		log.Println(msg)
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
}

func TestGetFirstEvent(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetFirstEvent")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	msg1 := "{\"message\":\"First event\"}"
	evStore.Committer("test").SubmitEvent("", "test", msg1)
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event\":" + strconv.Itoa(n) + "}"
		evStore.Committer("test").SubmitEvent("", "test", msg2)
	}
	p := make(map[string]interface{})
	p["id"] = 1
	p["tag"] = "test"
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (interface{}, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c.(chan string) {
		msgCounter = msgCounter + 1
		log.Println(msg)
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if m["event"].(map[string]interface{})["message"] != "First event" {
			t.Fatal("Incorrect message retuned: ", m["message"])
		}
	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
	if msgCounter > 1 {
		t.Fatal("Too many messages returned", msgCounter)
	}
}

func TestGetFirstEventByType(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetFirstEvent")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event before\":" + strconv.Itoa(n) + "}"
		evStore.Committer("test").SubmitEvent("", "scalar", msg2)
	}
	msg1 := "{\"message\":\"First event of type test\"}"
	evStore.Committer("test").SubmitEvent("", "test", msg1)
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event\":" + strconv.Itoa(n) + "}"
		evStore.Committer("test").SubmitEvent("", "scalar", msg2)
	}
	p := make(map[string]interface{})
	p["tag"] = "test"
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (interface{}, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c.(chan string) {
		msgCounter = msgCounter + 1
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if m["event"].(map[string]interface{})["message"] != "First event of type test" {
			t.Fatal("Incorrect message retuned: ", m["message"])
		}
	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
	if msgCounter > 1 {
		t.Fatal("Too many messages returned", msgCounter)
	}

}
func TestGetFirstEventByFilter(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	submitNScalars(evStore, "scalar", 10, 1, 1, 0)
	submitNScalars(evStore, "scalar", 3, 2, 1, 0)
	submitNScalars(evStore, "scalar", 100, 1, 1, 0)
	f, err := getRPCFunction(evStore, "GetFirstEvent")
	if err != nil {
		t.Fatal(err)
	}
	p := make(map[string]interface{})
	p["tag"] = "scalar"
	p["filter"] = "{\"event.box_id\": { \"$eq\": 2 }, \"event.var_id\": {\"$eq\": 1}}"
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (interface{}, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c.(chan string) {
		msgCounter = msgCounter + 1
		log.Println(msg)
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if m["event"].(map[string]interface{})["box_id"].(float64) != float64(2) {
			t.Fatal("Incorrect message retuned: ", m["event"])
		}
		if m["event"].(map[string]interface{})["var_id"].(float64) != float64(1) {
			t.Fatal("Incorrect message retuned: ", m["event"])
		}
		if m["event"].(map[string]interface{})["value"].(float64) != float64(0) {
			t.Fatal("Incorrect message retuned: ", m["event"])
		}

	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
	if msgCounter > 1 {
		t.Fatal("Too many messages returned", msgCounter)
	}

}
func TestGetLastEventByFilter(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	submitNScalars(evStore, "scalar", 10, 1, 1, 0)
	submitNScalars(evStore, "scalar", 3, 2, 1, 0)
	submitNScalars(evStore, "scalar", 100, 1, 1, 0)
	f, err := getRPCFunction(evStore, "GetLastEvent")
	if err != nil {
		t.Fatal(err)
	}
	p := make(map[string]interface{})
	p["tag"] = "scalar"
	p["filter"] = "{\"event.box_id\": { \"$eq\": 2 }, \"event.var_id\": {\"$eq\": 1}}"
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (interface{}, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c.(chan string) {
		msgCounter = msgCounter + 1
		log.Println(msg)
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if m["event"].(map[string]interface{})["box_id"].(float64) != float64(2) {
			t.Fatal("Incorrect message retuned: ", m["event"])
		}
		if m["event"].(map[string]interface{})["var_id"].(float64) != float64(1) {
			t.Fatal("Incorrect message retuned: ", m["event"])
		}
		if m["event"].(map[string]interface{})["value"].(float64) != float64(2) {
			t.Fatal("Incorrect message retuned: ", m["event"])
		}

	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
	if msgCounter > 1 {
		t.Fatal("Too many messages returned", msgCounter)
	}

}

func TestGetLastEvent(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetLastEvent")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	msg1 := "{\"message\":\"First event\"}"
	evStore.Committer("test").SubmitEvent("", "test", msg1)
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event\":" + strconv.Itoa(n) + "}"
		evStore.Committer("test").SubmitEvent("", "test", msg2)
	}
	msgLast := "{\"message\":\"Last event\"}"
	evStore.Committer("test").SubmitEvent("", "test", msgLast)
	p := make(map[string]interface{})
	p["tag"] = "test"
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (interface{}, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c.(chan string) {
		msgCounter = msgCounter + 1
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if m["event"].(map[string]interface{})["message"] != "Last event" {
			t.Fatal("Incorrect message retuned: ", m["message"])
		}
	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
	if msgCounter > 1 {
		t.Fatal("Too many messages returned", msgCounter)
	}

}
func TestGetLastEventByType(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetLastEvent")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event before\":" + strconv.Itoa(n) + "}"
		evStore.Committer("test").SubmitEvent("", "test", msg2)
	}
	msg1 := "{\"message\":\"Last event of type test\"}"
	evStore.Committer("test").SubmitEvent("", "test", msg1)
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event\":" + strconv.Itoa(n) + "}"
		evStore.Committer("test").SubmitEvent("", "scalar", msg2)
	}
	p := make(map[string]interface{})
	p["tag"] = "test"
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (interface{}, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c.(chan string) {
		msgCounter = msgCounter + 1
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if m["event"].(map[string]interface{})["message"] != "Last event of type test" {
			t.Fatal("Incorrect message retuned: ", m["message"])
		}
	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
	if msgCounter > 1 {
		t.Fatal("Too many messages returned", msgCounter)
	}
}
func TestGetEventAt(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetEventAt")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	msg1 := "{\"message\":\"First event\"}"
	evStore.Committer("test").SubmitEvent("", "test", msg1)
	for n := 0; n < 50; n++ {
		msg2 := "{\"Counter\":" + strconv.Itoa(n) + "}"
		evStore.Committer("test").SubmitEvent("", "test", msg2)
	}
	tPoint := time.Now()
	<-time.After(100 * time.Millisecond)
	for n := 50; n < 100; n++ {
		msg2 := "{\"Counter\":" + strconv.Itoa(n) + "}"
		evStore.Committer("test").SubmitEvent("", "test", msg2)
	}
	msgLast := "{\"message\":\"Last event\"}"
	evStore.Committer("test").SubmitEvent("", "test", msgLast)
	p := make(map[string]interface{})
	p["tag"] = "test"
	p["at"] = tPoint
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (chan string, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c {
		log.Println(msg)
		msgCounter = msgCounter + 1
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if int(m["event"].(map[string]interface{})["Counter"].(float64)) != 49 {
			t.Fatal("Incorrect message retuned: ", m["event"])
		}
	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
	if msgCounter > 1 {
		t.Fatal("Too many messages returned", msgCounter)
	}

}
func TestGetEventAtByType(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetEventAt")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	for n := 0; n < 100; n++ {
		msg2 := "{\"Counter\":" + strconv.Itoa(n) + "}"
		evStore.Committer("test").SubmitEvent("", "test", msg2)
		evStore.Committer("test").SubmitEvent("", "fake", msg2)
	}
	tPoint := time.Now()
	<-time.After(100 * time.Millisecond)
	msg1 := "{\"message\":\"Last event of type test\"}"
	evStore.Committer("test").SubmitEvent("", "test", msg1)
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event\":" + strconv.Itoa(n) + "}"
		evStore.Committer("test").SubmitEvent("", "scalar", msg2)
		evStore.Committer("test").SubmitEvent("", "test", msg2)
	}
	p := make(map[string]interface{})
	p["tag"] = "test"
	p["at"] = tPoint
	p["filter"] = nil
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (chan string, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c {
		msgCounter = msgCounter + 1
		log.Println(msg)
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if int(m["event"].(map[string]interface{})["Counter"].(float64)) != 99 {
			t.Fatal("Incorrect message retuned: ", m["message"])
		}
	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
	if msgCounter > 1 {
		t.Fatal("Too many messages returned", msgCounter)
	}

}

func TestGetEventAtByFilter(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "GetEventAt")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	submitNScalars(evStore, "scalar", 100, 1, 1, 0)
	submitNScalars(evStore, "scalar", 1, 2, 2, 0)
	tPoint := time.Now()
	<-time.After(100 * time.Millisecond)
	submitNScalars(evStore, "scalar", 100, 3, 23, 0)
	p := make(map[string]interface{})
	p["tag"] = "scalar"
	p["at"] = tPoint
	pFilter := make(map[string]interface{}, 1)
	err = json.Unmarshal([]byte("{\"event.box_id\": { \"$eq\": 2 }, \"event.var_id\": {\"$eq\": 2}}"), &pFilter)
	if err != nil {
		t.Fatal(err)
	}
	p["filter"] = pFilter
	prms := NewRPCParameterInterface(p)
	c, err := f.(func(RPCParameterInterface) (chan string, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c {
		msgCounter = msgCounter + 1
		log.Println(msg)
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if m["event"].(map[string]interface{})["box_id"].(float64) != float64(2) {
			t.Fatal("Incorrect message retuned: ", m["event"])
		}
		if m["event"].(map[string]interface{})["var_id"].(float64) != float64(2) {
			t.Fatal("Incorrect message retuned: ", m["event"])
		}
		if m["event"].(map[string]interface{})["value"].(float64) != float64(0) {
			t.Fatal("Incorrect message retuned: ", m["event"])
		}
	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
	if msgCounter > 1 {
		t.Fatal("Too many messages returned", msgCounter)
	}

}

func TestListDatabases(t *testing.T) {
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "ListDatabases")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	dbList, err := f.(func(RPCParameterInterface) ([]string, error))(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(dbList) == 0 {
		t.Fatal("List should not be empty")
	}
}

func TestEcho(t *testing.T) {
	var m map[string]interface{}
	p := make(map[string]interface{}, 1)
	p["int"] = 1
	p["string"] = "This is string"
	prms := NewRPCParameterInterface(p)
	evStore, err := initEventStore("localhost", dbName)
	if err != nil {
		t.Fatal(err)
	}
	f, err := getRPCFunction(evStore, "Echo")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	c, err := f.(func(RPCParameterInterface) (interface{}, error))(prms)
	if err != nil {
		t.Fatal(err)
	}
	msgCounter := 0
	for msg := range c.(chan string) {
		msgCounter = msgCounter + 1
		log.Println(msg)
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		if int(m["int"].(float64)) != 1 {
			t.Fatal("Incorrect message retuned: ", m["int"])
		}
		if m["string"] != "This is string" {
			t.Fatal("Incorrect message retuned: ", m["string"])
		}
	}
	if msgCounter == 0 {
		t.Fatal("No message recieved from database")
	}
	if msgCounter > 1 {
		t.Fatal("Too many messages returned", msgCounter)
	}

}
