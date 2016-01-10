package main

import
//	"labix.org/v2/mgo"
(
	"encoding/json"
	"flag"
	"log"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type (
	// Datestamp type definition
	Datestamp int64

	// ScalarValue stores one scalar value for SystemState
	ScalarValue struct {
		VarID     int
		BoxID     int
		Value     float32
		TimePoint Datestamp
	}

	// VectorValue stores one vector for SystemState
	VectorValue struct {
		VecID     int
		BoxID     int
		TimePoint Datestamp
	}

	// SystemState is type to preserve current system state and track changes with the time
	SystemState struct {
		scalar ScalarValue
	}
	// EventReader interface DI for Event reader
	EventReader interface {
		Dial(url string) error
		SetEventStore(dbName string, colName string)
		ReadEvents(fromId interface{}, outQueue chan string) error
		Close()
	}
	//EventWriter interface DI for event submission to event store
	EventWriter interface {
		Dial(url string) error
		SetEventStore(dbName string, colName string)
		CommitEvent(eventJSON string) error
		Close()
	}
	// MongoEventReader implements reading events from MongoDB storage
	MongoEventReader struct {
		session              *mgo.Session
		dbName               string
		eventStoreCollection string
	}
	// MongoEventWriter implements event submission to MongoDB storage
	MongoEventWriter struct {
		session              *mgo.Session
		dbName               string
		eventStoreCollection string
	}
)

var sysState SystemState

func loadSystemState(eventSource <-chan string) {
	sysState.scalar.BoxID = 1
	sysState.scalar.Value = 1.0
	sysState.scalar.VarID = 1
	sysState.scalar.TimePoint = 1
}

// NewMongoEventReader makes new object
func NewMongoEventReader() EventReader {
	return &MongoEventReader{}
}

// Dial connect to server by Mongo URL
func (e *MongoEventReader) Dial(url string) error {
	var err error
	e.session, err = mgo.Dial(url)
	if err != nil {
		return err
	}
	return nil
}

// SetEventStore define database name and collection name to store events
func (e *MongoEventReader) SetEventStore(dbName string, colName string) {
	e.dbName = dbName
	e.eventStoreCollection = colName
}

// ReadEvent Read JSON events started from fromId to slice of strings
func (e *MongoEventReader) ReadEvents(fromId interface{}, outQueue chan string) error {
	var (
		iter   *mgo.Iter
		result interface{}
	)
	if fromId != nil {
		objId := bson.ObjectIdHex(fromId.(string))
		iter = e.session.DB(e.dbName).C(e.eventStoreCollection).Find(bson.M{"_id": bson.M{"$gt": objId}}).Iter()
		//		iter = e.session.DB(e.dbName).C(e.eventStoreCollection).FindId(objId).Iter()
	} else {
		iter = e.session.DB(e.dbName).C(e.eventStoreCollection).Find(nil).Iter()
	}
	go func() {
		for iter.Next(&result) {
			s, err := json.Marshal(result)
			if err != nil {
				return
			}
			outQueue <- string(s)
		}
		iter.Close()
		close(outQueue)
	}()
	return nil
}

// Close closes connection to the server
func (e *MongoEventReader) Close() {
	e.session.Close()
}

// NewMongoEventWriter make new object and returns pointer to interface
func NewMongoEventWriter() EventWriter {
	return &MongoEventWriter{}
}

// Dial connect to MongoDB storage
func (e *MongoEventWriter) Dial(url string) error {
	var err error
	e.session, err = mgo.Dial(url)
	if err != nil {
		return err
	}
	return nil
}

// SetEventStore sets database name and event store collection name to store events
func (e *MongoEventWriter) SetEventStore(dbName string, colName string) {
	e.dbName = dbName
	e.eventStoreCollection = colName
}

// CommitEvent commits one event to the data store
func (e *MongoEventWriter) CommitEvent(eventJSON string) error {
	var object interface{}
	err := json.Unmarshal([]byte(eventJSON), &object)
	if err != nil {
		return err
	}
	return e.session.DB(e.dbName).C(e.eventStoreCollection).Insert(object)
}

// Close closes connection to the MongoDB server
func (e *MongoEventWriter) Close() {
	e.session.Close()
}
func main() {
	var dbServer string
	flag.StringVar(&dbServer, "server", "", "Path to database mongo://localhost")
	realReader := NewMongoEventReader()
	err := realReader.Dial(dbServer)
	if err != nil {
		log.Fatal(err)
	}
	//loadSystemState(eventRead)
}
