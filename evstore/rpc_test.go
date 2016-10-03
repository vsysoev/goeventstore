package main

import (
	"log"
	"net/rpc/jsonrpc"
	"testing"
	"time"
)

func runServer() {
	go main()
	<-time.After(100 * time.Millisecond)
}

func TestRPC(t *testing.T) {
	runServer()
	t.Run("Hello world", func(t *testing.T) {
		var (
			reply string
		)
		client, err := jsonrpc.Dial("tcp", "127.0.0.1:1234")
		if err != nil {
			t.Fatal("dialing:", err)
		}
		if client == nil {
			t.Fatal("Client should be nil")
		}

		err = client.Call("RPC.Echo", "Hello world!", &reply)
		if err != nil {
			t.Fatal(err)
		}
		if reply != "Hello world!" {
			t.Fatal(reply + " != " + "Hello world!")
		}
		log.Println(reply)
		client.Close()
	})
	t.Run("Nil in reply should not panic", func(t *testing.T) {
		client, err := jsonrpc.Dial("tcp", "127.0.0.1:1234")
		if err != nil {
			t.Fatal("dialing:", err)
		}
		if client == nil {
			t.Fatal("Client should be nil")
		}
		log.Println("Before client call")
		err = client.Call("RPC.Echo", "Hello world!", nil)
		if err != nil {
			t.Fatal(err)
		}
		log.Println("After client call")
		client.Close()
	})
}
