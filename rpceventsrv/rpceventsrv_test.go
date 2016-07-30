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

func (f *fakeQuery) Find(params interface{}, sortOrder string) (chan string, error) {
	return nil, errors.New("Not implemented")
}

func (f *fakeQuery) FindOne(params interface{}, sortOrder string) (chan string, error) {
	return nil, errors.New("Not implemented")
}

func (f *fakeQuery) Pipe(fakePipeline interface{}) (chan string, error) {
	return nil, errors.New("Not implemented")
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
	c, err := evstore.Dial("localhost", dbName, "test")
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
	evStore, err := evstore.Dial("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	rpc := NewRPCFunctionInterface(evStore)
	if err != nil {
		t.Fatal(err)
	}
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
	f, err := rpc.GetFunction("Echo")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
}
func TestNegativeGetFunction(t *testing.T) {
	evStore, err := evstore.Dial("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	rpc := NewRPCFunctionInterface(evStore)
	if err != nil {
		t.Fatal(err)
	}
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
	f, err := rpc.GetFunction("EchoFake")
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
	f, err := rpc.GetFunction("FindLastEvent")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	_, err = (f.(func() (chan string, error)))()
	if err == nil {
		t.Fatal("Should be an error")
	}
	if err.Error() != "EventStore isn't connected" {
		t.Fatal(err)
	}
}
func TestGetLastEvent(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := evstore.Dial("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	rpc := NewRPCFunctionInterface(evStore)
	if err != nil {
		t.Fatal(err)
	}
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
	f, err := rpc.GetFunction("FindLastEvent")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	evStore.Manager().DropDatabase(dbName)
	notExpected := "{\"message\":\"NOT expected\"}"
	expected := "{\"message\":\"expected\"}"
	evStore.Committer().SubmitEvent("", "test", notExpected)
	evStore.Committer().SubmitEvent("", "test", expected)
	c, err := f.(func() (chan string, error))()
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msg, ok := <-c
	if !ok {
		t.Fatal("No message returned. Channel closed")
	}
	err = json.Unmarshal([]byte(msg), &m)
	if err != nil {
		t.Fatal(err)
	}
	if m["event"].(map[string]interface{})["message"] != "expected" {
		t.FailNow()
	}
}

func TestGetHistory(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := evstore.Dial("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	rpc := NewRPCFunctionInterface(evStore)
	if err != nil {
		t.Fatal(err)
	}
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
	f, err := rpc.GetFunction("GetHistory")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	evStore.Manager().DropDatabase(dbName)
	msg1 := "{\"message\":\"NOT expected\"}"
	evStore.Committer().SubmitEvent("", "test", msg1)
	<-time.After(2 * time.Second)
	tStart := time.Now()
	for n := 0; n < 10; n++ {
		fmt.Printf(".")
		msg2 := "{\"message\":\"" + strconv.Itoa(n) + "\"}"
		evStore.Committer().SubmitEvent("", "test", msg2)
		<-time.After(500 * time.Millisecond)
	}
	tStop := time.Now()
	<-time.After(2 * time.Second)
	evStore.Committer().SubmitEvent("", "test", msg1)
	log.Println(tStart, " < ", tStop)
	c, err := f.(func(time.Time, time.Time) (chan string, error))(tStart, tStop)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	for {
		msg, ok := <-c
		if !ok {
			break
		}
		log.Println(msg)
		err = json.Unmarshal([]byte(msg), &m)
		if err != nil {
			t.Fatal(err)
		}
		//		if m["event"].(map[string]interface{})["message"] != "expected" {
		//			t.Fatal(m["event"].(map[string]interface{})["message"])
		//		}
	}
}
func TestGetDistanceValueIncorrectPointNumber(t *testing.T) {
	evStore, err := evstore.Dial("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	rpc := NewRPCFunctionInterface(evStore)
	if err != nil {
		t.Fatal(err)
	}
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
	f, err := rpc.GetFunction("GetDistanceValue")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	tStart := time.Now()
	tStop := tStart.Add(10 * time.Second)
	_, err = f.(func(time.Time, time.Time, int) (chan string, error))(tStart, tStop, -10)
	expectedError := "numberOfPoints should be positive integer"
	if err.Error() != expectedError {
		t.Fatal(err.Error() + " != " + expectedError)
	}

}

func TestGetDistanceValueIncorrectInterval(t *testing.T) {
	evStore, err := evstore.Dial("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	rpc := NewRPCFunctionInterface(evStore)
	if err != nil {
		t.Fatal(err)
	}
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
	f, err := rpc.GetFunction("GetDistanceValue")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	tStop := time.Now()
	tStart := tStop.Add(10 * time.Second)
	_, err = f.(func(time.Time, time.Time, int) (chan string, error))(tStart, tStop, 10)
	expectedError := "To must be greater than From"
	if err.Error() != expectedError {
		t.Fatal(err.Error() + " != " + expectedError)
	}

}
func TestGetDistanceValueZerroInterval(t *testing.T) {
	evStore, err := evstore.Dial("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	rpc := NewRPCFunctionInterface(evStore)
	if err != nil {
		t.Fatal(err)
	}
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
	f, err := rpc.GetFunction("GetDistanceValue")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	tStart := time.Now()
	_, err = f.(func(time.Time, time.Time, int) (chan string, error))(tStart, tStart, 10)
	expectedError := "To must be greater than From"
	if err.Error() != expectedError {
		t.Fatal(err.Error() + " != " + expectedError)
	}

}

func TestGetDistanceValue(t *testing.T) {
	var (
		m map[string]interface{}
	)
	evStore, err := evstore.Dial("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	rpc := NewRPCFunctionInterface(evStore)
	if err != nil {
		t.Fatal(err)
	}
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
	f, err := rpc.GetFunction("GetDistanceValue")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	evStore.Manager().DropDatabase(dbName)
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
	c, err := f.(func(time.Time, time.Time, int) (chan string, error))(tStart, tStop, 10)
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
	evStore, err := evstore.Dial("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	rpc := NewRPCFunctionInterface(evStore)
	if err != nil {
		t.Fatal(err)
	}
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
	f, err := rpc.GetFunction("GetFirstEvent")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	evStore.Manager().DropDatabase(dbName)
	msg1 := "{\"message\":\"First event\"}"
	evStore.Committer().SubmitEvent("", "test", msg1)
	for n := 0; n < 100; n++ {
		msg2 := "{\"Fake event\":" + strconv.Itoa(n) + "}"
		evStore.Committer().SubmitEvent("", "test", msg2)
		fmt.Print(".")
	}
	c, err := f.(func(tag string) (chan string, error))("test")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	log.Println("Before start reading message from channel")
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
	evStore, err := evstore.Dial("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	rpc := NewRPCFunctionInterface(evStore)
	if err != nil {
		t.Fatal(err)
	}
	if rpc == nil {
		t.Fatal("RPCFunctionInterface is nil")
	}
	f, err := rpc.GetFunction("GetFirstEvent")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("Function is nil")
	}
	evStore.Manager().DropDatabase(dbName)
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
	c, err := f.(func(tag string) (chan string, error))("test")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("Channel shouldn't be nil")
	}
	msgCounter := 0
	log.Println("Before start reading message from channel")
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
