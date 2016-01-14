package evstore

import
//	"labix.org/v2/mgo"
(
	"encoding/json"
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type (
	// EventReader interface DI for Event reader
	EventReader interface {
		Dial(url string, dbName string, eventCollection string) error
		Subscribe(fromID string) (chan string, error)
		Close()
	}
	//EventWriter interface DI for event submission to event store
	EventWriter interface {
		Dial(url string, dbName string, eventCollection string) error
		CommitEvent(eventJSON string) error
		Close()
	}
	// MongoEventReader implements reading events from MongoDB storage
	MongoEventReader struct {
		session           *mgo.Session
		dbName            string
		eventCollection   string
		triggerCollection string
		currentEvents     *chan string
	}
	// MongoEventWriter implements event submission to MongoDB storage
	MongoEventWriter struct {
		session           *mgo.Session
		dbName            string
		eventCollection   string
		triggerCollection string
	}
)

// NewMongoEventReader makes new object
func NewMongoEventReader() EventReader {
	return &MongoEventReader{}
}

// Dial connect to server by Mongo URL
func (e *MongoEventReader) Dial(url string, dbName string, eventCollection string) error {
	var err error
	e.session, err = mgo.Dial(url)
	if err != nil {
		return err
	}
	e.dbName = dbName
	e.eventCollection = eventCollection
	e.triggerCollection = eventCollection + "_capped"
	return nil
}

// ReadEvents Read JSON events started from fromId to slice of strings
func (e *MongoEventReader) readEvents(fromID string) (chan string, error) {
	var (
		iter   *mgo.Iter
		result interface{}
	)
	outQueue := make(chan string)
	if fromID != "" {
		objID := bson.ObjectIdHex(fromID)
		iter = e.session.DB(e.dbName).C(e.eventCollection).Find(bson.M{"_id": bson.M{"$gt": objID}}).Iter()
	} else {
		iter = e.session.DB(e.dbName).C(e.eventCollection).Find(nil).Iter()
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
	return outQueue, nil
}

// Subscribe get channels
func (e *MongoEventReader) Subscribe(fromID string) (chan string, error) {
	return e.readEvents(fromID)
}

// Close closes connection to the server
func (e *MongoEventReader) Close() {
	e.session.Close()
}

// NewMongoEventWriter make new object and returns pointer to interface
func NewMongoEventWriter() EventWriter {
	return &MongoEventWriter{}
}

func contains(col []string, target string) bool {
	for _, s := range col {
		if s == target {
			return true
		}
	}
	return false
}

// Dial connect to MongoDB storage
func (e *MongoEventWriter) Dial(url string, dbName string, eventCollection string) error {
	var err error
	e.session, err = mgo.Dial(url)
	if err != nil {
		return err
	}
	e.eventCollection = eventCollection
	e.triggerCollection = eventCollection + "_capped"
	collections, err := e.session.DB(dbName).CollectionNames()
	fmt.Println("Collections: ", collections)
	if err != nil {
		return err
	}
	if !contains(collections, e.triggerCollection) {
		cInfo := mgo.CollectionInfo{
			Capped:   true,
			MaxBytes: 1000000,
		}
		e.session.DB(dbName).C(e.triggerCollection).Create(&cInfo)
	}
	return nil
}

// CommitEvent commits one event to the data store
func (e *MongoEventWriter) CommitEvent(eventJSON string) error {
	var object map[string]interface{}
	err := json.Unmarshal([]byte(eventJSON), &object)
	if err != nil {
		return err
	}
	event := make(map[string]interface{})
	event["event"] = object
	err = e.session.DB(e.dbName).C(e.eventCollection).Insert(event)
	if err != nil {
		return err
	}
	err = e.session.DB(e.dbName).C(e.triggerCollection).Insert(bson.M{"trigger": 1})
	return err
}

// Close closes connection to the MongoDB server
func (e *MongoEventWriter) Close() {
	e.session.Close()
}
