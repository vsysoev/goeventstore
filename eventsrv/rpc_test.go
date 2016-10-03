package main

import (
	"log"
	"testing"
	"time"

	"github.com/powerman/rpc-codec/jsonrpc2"
	"github.com/vsysoev/goeventstore/property"
)

func runServer() {
	go main()
	<-time.After(100 * time.Millisecond)
}

func TestRPC(t *testing.T) {
	runServer()
	props := property.Init()

	t.Run("Hello world", func(t *testing.T) {
		type (
			NameArg struct {
				Msg string
			}
		)
		var (
			inp, reply NameArg
		)
		// Client use HTTP transport.
		client := jsonrpc2.NewHTTPClient("http://127.0.0.1" + props["events.url"] + "/rpc")
		defer client.Close()
		if client == nil {
			t.Fatal("Client should be nil")
		}
		inp = NameArg{"Hello world!"}
		err := client.Call("RPC.Echo", inp, &reply)
		if err != nil {
			t.Fatal(err)
		}
		if reply.Msg != inp.Msg {
			t.Fatal(reply.Msg + " != " + inp.Msg)
		}
		log.Println(reply)
		client.Close()
	})
	t.Run("Nil in reply should not panic", func(t *testing.T) {
		client := jsonrpc2.NewHTTPClient("http://127.0.0.1" + props["events.url"] + "/rpc")
		defer client.Close()
		if client == nil {
			t.Fatal("Client should be nil")
		}
		err := client.Call("RPC.Echo", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		client.Close()
	})
	t.Run("SubmitEvent", func(t *testing.T) {
		type MessageArg struct {
			Stream  string
			SeqID   string
			Tag     string
			Payload string
		}
		var reply bool
		client := jsonrpc2.NewHTTPClient("http://127.0.0.1" + props["events.url"] + "/rpc")
		defer client.Close()
		if client == nil {
			t.Fatal("Client should be nil")
		}
		msg := MessageArg{}
		msg.Stream = "test"
		msg.SeqID = ""
		msg.Tag = "scalar"
		msg.Payload = "{\"var_id\": 1, \"value\":12.5}"
		log.Println("Msg is ", msg)
		err := client.Call("RPC.Submit", msg, &reply)
		if err != nil {
			t.Fatal(err)
		}
		client.Close()
	})

}
