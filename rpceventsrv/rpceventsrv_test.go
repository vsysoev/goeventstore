package main

import (
	"testing"
)

func TestNilEventStore(t *testing.T) {
	var (
		rpc RPCFunction
	)
	if rpc.evStore != nil {
		t.Fatal("Error. evStore initialed but should not")
	}
	ch, err := rpc.FindLastEvent("")
	if err != nil {
		t.Fatal(err)
	}
	if ch == nil {
		t.Fatal("Channel is nil")
	}
}
