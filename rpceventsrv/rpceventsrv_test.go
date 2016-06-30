package main

import (
	"log"
	"testing"
)

func TestNilEventStore(t *testing.T) {
	var (
		rpc RPCFunction
	)
	log.Println(rpc.evStore)
	if rpc.evStore != nil {
		t.Fatal("Error. evStore initialed but should not")
	}
}

/*
func TestGetLastEvent(t *testing.T) {
	var rpc RPCFunction

	ch, err := rpc.FindLastEvent("")
	if err != nil {
		t.Fatal(err)
	}
	if ch == nil {
		t.Fatal("Channel is nil")
	}
}
*/
