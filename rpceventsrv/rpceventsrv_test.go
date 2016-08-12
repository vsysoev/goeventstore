package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

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
func (f *fakeDB) Committer() evstore.Committer {
	return &fakeCommitter{}
}
func (f *fakeDB) Listenner() evstore.Listenner {
	return &fakeListenner{}
}
func (f *fakeDB) Listenner2() evstore.Listenner2 {
	return &fakeListenner2{}
}
func (f *fakeDB) Manager() evstore.Manager {
	return &fakeManager{}
}
func (f *fakeDB) Query() evstore.Query {
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

func (f *fakeListenner) Subscribe(fromID string) (chan string, error) {
	return nil, errors.New("Not implemented")
}
func (f *fakeListenner) Unsubscribe(eventChannel chan string) {

}

func (f *fakeListenner2) Subscribe2(eventTypes string, handlerFunc evstore.Handler) error {
	return errors.New("Not implemented")
}
func (f *fakeListenner2) Unsubscribe2(eventTypes string) {

}
func (f *fakeListenner2) GetLastID() string {
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

func (f *fakeQuery) Find(params interface{}, sortOrder string) (chan string, error) {
	return nil, errors.New("Not implemented")
}

func (f *fakeQuery) FindOne(params interface{}, sortOrder string) (chan string, error) {
	return nil, errors.New("Not implemented")
}

func (f *fakeQuery) Pipe(fakePipeline interface{}) (chan string, error) {
	return nil, errors.New("Not implemented")
}

func initEventStore(url string, dbName string, collectionName string) (evstore.Connection, error) {
	evStore, err := evstore.Dial(url, dbName, collectionName)
	if err != nil {
		return nil, err
	}
	evStore.Manager().DropDatabase(dbName)
	return evStore, err
}

func submitNScalars(evStore evstore.Connection, number int, box_id int, var_id int, delay time.Duration) error {
	for n := 0; n < number; n++ {
		msg := "{\"box_id\":" + strconv.Itoa(box_id) + ", \"var_id\":" + strconv.Itoa(var_id) + ", \"value\":" + strconv.FormatFloat(float64(n), 'f', -1, 32) + "}"
		err := evStore.Committer().SubmitEvent("", "scalar", msg)
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
	c, err := initEventStore("localhost", dbName, "test")
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
	_, err = (f.(func(string, string) (chan string, error)))("", "")
	if err == nil {
		t.Fatal("Should be an error")
	}
	if err.Error() != "EventStore isn't connected" {
		t.Fatal(err)
	}
}

func TestGetHistory(t *testing.T) {
	evStore, err := initEventStore("localhost", dbName, "test")
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
	evStore.Committer().SubmitEvent("", "scalar", msg1)
	<-time.After(1 * time.Second)
	tStart := time.Now()
	<-time.After(1 * time.Second)
	submitNScalars(evStore, 10, 1, 1, 100*time.Millisecond)
	tStop := time.Now()
	<-time.After(2 * time.Second)
	evStore.Committer().SubmitEvent("", "scalar", msg1)
	log.Println(tStart, " < ", tStop)
	c, err := f.(func(string, time.Time, time.Time, string) (chan string, error))("scalar", tStart, tStop, "")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	expected := 0
	msgCount := 0
	for msg := range c {
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
	evStore, err := initEventStore("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	msg1 := "{\"message\":\"NOT expected\"}"
	evStore.Committer().SubmitEvent("", "scalar", msg1)
	<-time.After(1 * time.Second)
	tStart := time.Now()
	<-time.After(1 * time.Second)
	submitNScalars(evStore, 10, 1, 1, 100*time.Millisecond)
	submitNScalars(evStore, 20, 1, 2, 100*time.Millisecond)
	tStop := time.Now()
	<-time.After(2 * time.Second)
	evStore.Committer().SubmitEvent("", "scalar", msg1)
	log.Println(tStart, " < ", tStop)
	f, err := getRPCFunction(evStore, "GetHistory")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	c, err := f.(func(string, time.Time, time.Time, string) (chan string, error))("scalar", tStart, tStop, "{\"event.box_id\": { \"$eq\": 1 }, \"event.var_id\": {\"$eq\": 2}}")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	expected := 0
	msgCount := 0
	for msg := range c {
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
	_, err = f.(func(string, time.Time, time.Time, int, string) (chan string, error))("scalar", tStart, tStop, -10, "")
	expectedError := "numberOfPoints should be positive integer"
	if err.Error() != expectedError {
		t.Fatal(err.Error() + " != " + expectedError)
	}

}

func TestGetDistanceValueIncorrectInterval(t *testing.T) {
	evStore, err := initEventStore("localhost", dbName, "test")
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
	_, err = f.(func(string, time.Time, time.Time, int, string) (chan string, error))("scalar", tStart, tStop, 10, "")
	expectedError := "To must be greater than From"
	if err.Error() != expectedError {
		t.Fatal(err.Error() + " != " + expectedError)
	}

}
func TestGetDistanceValueZerroInterval(t *testing.T) {
	evStore, err := initEventStore("localhost", dbName, "test")
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
	_, err = f.(func(string, time.Time, time.Time, int, string) (chan string, error))("scalar", tStart, tStart, 10, "")
	expectedError := "To must be greater than From"
	if err.Error() != expectedError {
		t.Fatal(err.Error() + " != " + expectedError)
	}

}

func TestGetDistanceValue(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := initEventStore("localhost", dbName, "test")
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
	evStore.Committer().SubmitEvent("", "test", msg1)
	<-time.After(1 * time.Second)
	tStart := time.Now()
	<-time.After(10 * time.Millisecond)
	for n := 0; n < 100; n++ {
		fmt.Printf(".")
		msg2 := "{\"value\":" + strconv.Itoa(n) + "}"
		evStore.Committer().SubmitEvent("", "scalar", msg2)
		<-time.After(100 * time.Millisecond)
	}
	tStop := time.Now()
	<-time.After(2 * time.Second)
	evStore.Committer().SubmitEvent("", "test", msg1)
	log.Println(tStart, " < ", tStop)
	c, err := f.(func(string, time.Time, time.Time, int, string) (chan string, error))("scalar", tStart, tStop, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for {
		msg, ok := <-c
		if !ok {
			break
		}
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
	evStore.Committer().SubmitEvent("", "test", msg1)
	<-time.After(1 * time.Second)
	tStart := time.Now()
	<-time.After(10 * time.Millisecond)
	submitNScalars(evStore, 100, 1, 1, 100*time.Millisecond)
	submitNScalars(evStore, 100, 3, 1, 0)
	tStop := time.Now()
	<-time.After(2 * time.Second)
	evStore.Committer().SubmitEvent("", "test", msg1)
	log.Println(tStart, " < ", tStop)
	c, err := f.(func(string, time.Time, time.Time, int, string) (chan string, error))("scalar", tStart, tStop, 10, "{\"event.box_id\": { \"$eq\": 1 }, \"event.var_id\": {\"$eq\": 1}}")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for {
		msg, ok := <-c
		if !ok {
			break
		}
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
	evStore.Committer().SubmitEvent("", "test", msg1)
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event\":" + strconv.Itoa(n) + "}"
		evStore.Committer().SubmitEvent("", "test", msg2)
	}
	c, err := f.(func(string, string) (chan string, error))("test", "")
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
		evStore.Committer().SubmitEvent("", "scalar", msg2)
	}
	msg1 := "{\"message\":\"First event of type test\"}"
	evStore.Committer().SubmitEvent("", "test", msg1)
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event\":" + strconv.Itoa(n) + "}"
		evStore.Committer().SubmitEvent("", "scalar", msg2)
	}
	c, err := f.(func(string, string) (chan string, error))("test", "")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c {
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
	evStore, err := initEventStore("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	submitNScalars(evStore, 10, 1, 1, 0)
	submitNScalars(evStore, 3, 2, 1, 0)
	submitNScalars(evStore, 100, 1, 1, 0)
	f, err := getRPCFunction(evStore, "GetFirstEvent")
	if err != nil {
		t.Fatal(err)
	}
	c, err := f.(func(string, string) (chan string, error))("scalar", "{\"event.box_id\": { \"$eq\": 2 }, \"event.var_id\": {\"$eq\": 1}}")
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
	evStore, err := initEventStore("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	submitNScalars(evStore, 10, 1, 1, 0)
	submitNScalars(evStore, 3, 2, 1, 0)
	submitNScalars(evStore, 100, 1, 1, 0)
	f, err := getRPCFunction(evStore, "GetLastEvent")
	if err != nil {
		t.Fatal(err)
	}
	c, err := f.(func(string, string) (chan string, error))("scalar", "{\"event.box_id\": { \"$eq\": 2 }, \"event.var_id\": {\"$eq\": 1}}")
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
	evStore.Committer().SubmitEvent("", "test", msg1)
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event\":" + strconv.Itoa(n) + "}"
		evStore.Committer().SubmitEvent("", "test", msg2)
	}
	msgLast := "{\"message\":\"Last event\"}"
	evStore.Committer().SubmitEvent("", "test", msgLast)
	c, err := f.(func(string, string) (chan string, error))("test", "")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c {
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
		evStore.Committer().SubmitEvent("", "test", msg2)
	}
	msg1 := "{\"message\":\"Last event of type test\"}"
	evStore.Committer().SubmitEvent("", "test", msg1)
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event\":" + strconv.Itoa(n) + "}"
		evStore.Committer().SubmitEvent("", "scalar", msg2)
	}
	c, err := f.(func(string, string) (chan string, error))("test", "")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	for msg := range c {
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
	evStore.Committer().SubmitEvent("", "test", msg1)
	for n := 0; n < 50; n++ {
		msg2 := "{\"Counter\":" + strconv.Itoa(n) + "}"
		evStore.Committer().SubmitEvent("", "test", msg2)
	}
	tPoint := time.Now()
	<-time.After(100 * time.Millisecond)
	for n := 50; n < 100; n++ {
		msg2 := "{\"Counter\":" + strconv.Itoa(n) + "}"
		evStore.Committer().SubmitEvent("", "test", msg2)
	}
	msgLast := "{\"message\":\"Last event\"}"
	evStore.Committer().SubmitEvent("", "test", msgLast)
	c, err := f.(func(string, time.Time, string) (chan string, error))("", tPoint, "")
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
		evStore.Committer().SubmitEvent("", "test", msg2)
		evStore.Committer().SubmitEvent("", "fake", msg2)
	}
	tPoint := time.Now()
	<-time.After(100 * time.Millisecond)
	msg1 := "{\"message\":\"Last event of type test\"}"
	evStore.Committer().SubmitEvent("", "test", msg1)
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event\":" + strconv.Itoa(n) + "}"
		evStore.Committer().SubmitEvent("", "scalar", msg2)
		evStore.Committer().SubmitEvent("", "test", msg2)
	}
	c, err := f.(func(string, time.Time, string) (chan string, error))("test", tPoint, "")
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
	submitNScalars(evStore, 100, 1, 2, 0)
	submitNScalars(evStore, 1, 2, 2, 0)
	tPoint := time.Now()
	<-time.After(100 * time.Millisecond)
	submitNScalars(evStore, 100, 3, 2, 0)
	c, err := f.(func(string, time.Time, string) (chan string, error))("scalar", tPoint, "{\"event.box_id\": { \"$eq\": 2 }, \"event.var_id\": {\"$eq\": 2}}")
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
	evStore, err := initEventStore("localhost", dbName, "test")
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
	dbList, err := f.(func() ([]string, error))()
	if err != nil {
		t.Fatal(err)
	}
	if len(dbList) == 0 {
		t.Fatal("List should not be empty")
	}
}
