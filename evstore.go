package evstore

import
//	"labix.org/v2/mgo"
(
	"encoding/json"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type (
	// EventReader interface DI for Event reader
	EventReader interface {
		Dial(url string) error
		SetEventStore(dbName string, colName string)
		Subscribe(fromId int) (chan string, error)
		Close()
	}
	//EventWriter interface DI for event submission to event store
	EventWriter interface {
		Dial(url string) error
		SetEventStore(dbName string, eventColName string, idColName string)
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
		idStoreCollection    string
	}
)

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
func (e *MongoEventReader) ReadEvents(fromId int) (chan string, error) {
	var (
		iter   *mgo.Iter
		result interface{}
	)
	outQueue := make(chan string)
	if fromId >= 0 {
		iter = e.session.DB(e.dbName).C(e.eventStoreCollection).Find(bson.M{"eventid": bson.M{"$gt": fromId}}).Iter()
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
	return outQueue, nil
}

func (e *MongoEventReader) Subscribe(fromId int) (chan string, error) {
	return e.ReadEvents(fromId)
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
func (e *MongoEventWriter) SetEventStore(dbName string, eventColName string, idColName string) {
	e.dbName = dbName
	e.eventStoreCollection = eventColName
	e.idStoreCollection = idColName
}

// CommitEvent commits one event to the data store
func (e *MongoEventWriter) CommitEvent(eventJSON string) error {
	var object map[string]interface{}
	err := json.Unmarshal([]byte(eventJSON), &object)
	if err != nil {
		return err
	}
	change := mgo.Change{
		Update:    bson.M{"$inc": bson.M{"eventid": 1}},
		Upsert:    true,
		ReturnNew: true,
	}
	var idDoc map[string]interface{}
	_, err = e.session.DB(e.dbName).C(e.idStoreCollection).Find(nil).Apply(change, &idDoc)
	if err != nil {
		return err
	}
	event := make(map[string]interface{})
	event["eventid"] = idDoc["eventid"]
	event["event"] = object
	return e.session.DB(e.dbName).C(e.eventStoreCollection).Insert(event)
}

// Close closes connection to the MongoDB server
func (e *MongoEventWriter) Close() {
	e.session.Close()
}

// WebSocket handle
/*
func eventReader(ws *websocket.Conn) {
	var err error

	rd := NewMongoEventReader()
	err = rd.Dial("mongodb://localhost/test")
	if err != nil {
		panic("Can't connect mongodb server")
	}
	rd.SetEventStore("test", "events")
	for {
		var reply string

		if err = websocket.Message.Receive(ws, &reply); err != nil {
			fmt.Println("Can't receive")
			break
		}
		var jsonReply map[string]interface{}
		err = json.Unmarshal([]byte(reply), &jsonReply)
		var out chan string
		if jsonReply["_id"] != "" {
			out, err = rd.Subscribe(jsonReply["eventid"].(int))
		} else {
			out, err = rd.Subscribe(-1)
		}
		for s := range out {
			fmt.Println("SND", s)
			if err = websocket.Message.Send(ws, []byte(s)); err != nil {
				fmt.Println("Can't send")
				break
			}
		}
	}
}
*/
/*
func main() {
	http.Handle("/events", websocket.Handler(eventReader))
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		panic("Error start http server")
	}
}
*/
