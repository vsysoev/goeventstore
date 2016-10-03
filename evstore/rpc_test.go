package evstore

import (
	"log"
	"net/rpc"
	"testing"
	"time"
)

func TestRPC(t *testing.T) {
	var (
		reply string
	)
	go main()
	<-time.After(100 * time.Millisecond)
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
	if err != nil {
		t.Fatal("dialing:", err)
	}
	if client == nil {
		t.Fatal("Client should be nil")
	}

	err = client.Call("RPCType.HelloWorld", "Vladimir", &reply)
	if err != nil {
		t.Fatal(err)
	}
	if reply != "Hello world Vladimir!" {
		t.Fatal(reply + " != " + "Hello world Vladimir!")
	}
	log.Println(reply)
}
