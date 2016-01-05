package main

import
//	"labix.org/v2/mgo"
(
	"errors"
	"flag"
	"log"
)

// Datestamp type definition
type Datestamp int64

// ScalarValue stores one scalar value for SystemState
type ScalarValue struct {
	VarID     int
	BoxID     int
	Value     float32
	TimePoint Datestamp
}

// VectorValue stores one vector for SystemState
type VectorValue struct {
	VecID     int
	BoxID     int
	TimePoint Datestamp
}

// SystemState is type to preserve current system state and track changes with the time
type SystemState struct {
	scalar ScalarValue
}

var sysState SystemState

func loadSystemState(eventSource <-chan string) {
	sysState.scalar.BoxID = 1
	sysState.scalar.Value = 1.0
	sysState.scalar.VarID = 1
	sysState.scalar.TimePoint = 1
}
func eventReader(serverName string) (<-chan string, error) {
	return nil, errors.New("Not implemented")
}
func main() {
	var dbServer string
	flag.StringVar(&dbServer, "server", "", "Path to database mongo://localhost")
	eventRead, err := eventReader(dbServer)
	if err != nil {
		log.Fatal(err)
	}
	loadSystemState(eventRead)
}
