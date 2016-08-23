package main

import (
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/vsysoev/goeventstore/wsock"

	"golang.org/x/net/context"
	"golang.org/x/net/websocket"
)

type (
	fakeClient struct {
		fromWS chan *wsock.MessageT
		toWS   chan *wsock.MessageT
		done   chan bool
	}
)

func NewFakeClient() wsock.Connector {
	toWS := make(chan *wsock.MessageT, 1)
	fromWS := make(chan *wsock.MessageT, 1)
	done := make(chan bool, 1)
	return &fakeClient{fromWS, toWS, done}
}

func (f *fakeClient) GetChannels() (chan *wsock.MessageT, chan *wsock.MessageT, chan bool) {
	return f.fromWS, f.toWS, f.done
}

func (f *fakeClient) Request() *http.Request {
	return nil
}
func (f *fakeClient) Conn() *websocket.Conn {
	return nil
}
func (f *fakeClient) Write(msg *wsock.MessageT) {
}
func (f *fakeClient) Done() {

}

func TestClientHandler(t *testing.T) {
	var (
		f *RPCFunction
	)
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())

	go clientHandler(ctx, c, f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["method"] = "Echo"
	msg["params"] = make(map[string]interface{}, 1)
	msg["params"].(map[string]interface{})["int"] = 100
	msg["params"].(map[string]interface{})["string"] = "This is string param"
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	log.Println(m)
}

func TestClientGetHistory(t *testing.T) {
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())
	evStore, err := initEventStore("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	f := RPCFunction{evStore}
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

	go clientHandler(ctx, c, &f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["method"] = "GetHistory"
	msg["params"] = make(map[string]interface{}, 1)
	msg["params"].(map[string]interface{})["from"] = tStart
	msg["params"].(map[string]interface{})["to"] = tStop
	msg["params"].(map[string]interface{})["tag"] = "scalar"
	msg["params"].(map[string]interface{})["filter"] = ""
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	log.Println(m)
}

func TestClientGetLastEventByFilter(t *testing.T) {
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())
	evStore, err := initEventStore("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	f := RPCFunction{evStore}
	submitNScalars(evStore, 10, 1, 1, 0)
	submitNScalars(evStore, 3, 2, 1, 0)
	submitNScalars(evStore, 100, 1, 1, 0)
	go clientHandler(ctx, c, &f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["method"] = "GetLastEvent"
	msg["params"] = make(map[string]interface{}, 1)
	msg["params"].(map[string]interface{})["tag"] = "scalar"
	msg["params"].(map[string]interface{})["filter"] = "{\"event.box_id\": { \"$eq\": 2 }, \"event.var_id\": {\"$eq\": 1}}"
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	log.Println(m)
}

func TestClientGetFirstEventByFilter(t *testing.T) {
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())
	evStore, err := initEventStore("localhost", dbName, "test")
	if err != nil {
		t.Fatal(err)
	}
	f := RPCFunction{evStore}
	submitNScalars(evStore, 10, 1, 1, 0)
	submitNScalars(evStore, 3, 2, 1, 0)
	submitNScalars(evStore, 100, 1, 1, 0)
	go clientHandler(ctx, c, &f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["method"] = "GetFirstEvent"
	msg["params"] = make(map[string]interface{}, 1)
	msg["params"].(map[string]interface{})["tag"] = "scalar"
	msg["params"].(map[string]interface{})["filter"] = "{\"event.box_id\": { \"$eq\": 2 }, \"event.var_id\": {\"$eq\": 1}}"
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	log.Println(m)
}

func TestNegativeEcho(t *testing.T) {
	var (
		f *RPCFunction
	)
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())

	go clientHandler(ctx, c, f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["method"] = "Echo"
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	log.Println(m)
}
func TestNegativeNoMethod(t *testing.T) {
	var (
		f *RPCFunction
	)
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())

	go clientHandler(ctx, c, f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["id"] = 1
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	if m.String() != "{\"error\":\"ERROR: No method field found in request.\"}" {
		t.Fatal("Unexpected message returned: " + m.String())
	}
}
func TestNegativeMethodisnotstring(t *testing.T) {
	var (
		f *RPCFunction
	)
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())

	go clientHandler(ctx, c, f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["id"] = 1
	msg["method"] = 1
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	if m.String() != "{\"error\":\"ERROR: Method should be string. Not int\"}" {
		t.Fatal("Unexpected message returned: " + m.String())
	}
}
func TestMethodisstring(t *testing.T) {
	var (
		f *RPCFunction
	)
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())

	go clientHandler(ctx, c, f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["id"] = 1
	msg["method"] = "Echo"
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	if m.String() != "{\"error\":\"ERROR: Method should be string. Not int\"}" {
		t.Fatal("Unexpected message returned: " + m.String())
	}
}
func TestNegativeNoid(t *testing.T) {
	var (
		f *RPCFunction
	)
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())

	go clientHandler(ctx, c, f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["method"] = "Echo"
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	if m.String() != "{\"error\":\"ERROR: No id found in request.\"}" {
		t.Fatal("Unexpected message returned: " + m.String())
	}
}
func TestNegativeNointid(t *testing.T) {
	var (
		f *RPCFunction
	)
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())

	go clientHandler(ctx, c, f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["id"] = "1.0"
	msg["method"] = "Echo"
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	if m.String() != "{\"error\":\"ERROR: id should be int. Not string\"}" {
		t.Fatal("Unexpected message returned: " + m.String())
	}
}
func TestNegativeNoparams(t *testing.T) {
	var (
		f *RPCFunction
	)
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())

	go clientHandler(ctx, c, f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["id"] = 1
	msg["method"] = "Echo"
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	if m.String() != "{\"error\":\"ERROR: No params found in request.\"}" {
		t.Fatal("Unexpected message returned: " + m.String())
	}
}
func TestNegativeNoJSONparams(t *testing.T) {
	var (
		f *RPCFunction
	)
	c := NewFakeClient()
	ctx, cancelFunc := context.WithCancel(context.Background())

	go clientHandler(ctx, c, f)
	toClient, fromClient, _ := c.GetChannels()
	msg := wsock.MessageT{}
	msg["jsonrpc"] = "2.0"
	msg["id"] = 1
	msg["method"] = "Echo"
	msg["params"] = "{string}"
	toClient <- &msg
	m := <-fromClient
	cancelFunc()
	if m.String() != "{\"error\":\"ERROR: Params should be JSON object. Not string\"}" {
		t.Fatal("Unexpected message returned: " + m.String())
	}
}
