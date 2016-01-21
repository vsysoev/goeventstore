package evstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type (
	Connection struct {
		session           *mgo.Session
		dbName            string
		eventCollection   string
		triggerCollection string
		committer         *CommitterT
		listenner         *ListennerT
	}
	CommitterT struct {
		p *Connection
	}
	ListennerT struct {
		p *Connection
	}
	// EventReader interface DI for Event reader
	EventReader interface {
		Dial(url string, dbName string, eventCollection string) error
		Subscribe(fromID string) (chan string, error)
		Unsubscribe(eventChannel chan string)
		Close()
	}
	//EventWriter interface DI for event submission to event store
	EventWriter interface {
		Dial(url string, dbName string, eventCollection string) error
		CommitEvent(eventJSON string) error
		Close()
	}
	Committer interface {
		SubmitEvent(sequenceID string, eventJSON string) (string, error)
	}
	Listenner interface {
		Subscribe(fromID string) (chan string, error)
		Unsubscribe(eventChannel chan string)
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

func Dial(url string, dbName string, eventCollection string) (*Connection, error) {
	var err error
	c := Connection{}
	c.session, err = mgo.Dial(url)
	if err != nil {
		return nil, err
	}
	c.dbName = dbName
	c.eventCollection = eventCollection
	c.triggerCollection = eventCollection + "_capped"
	c.committer = &CommitterT{}
	c.listenner = &ListennerT{}
	c.listenner.p = &c
	c.committer.p = &c
	return &c, nil
}
func (c *Connection) Close() {
	c.session.Close()
}

func (c *Connection) Committer() Committer {
	return c.committer
}

func (c *Connection) Listenner() Listenner {
	return c.listenner
}
func (c *CommitterT) SubmitEvent(sequenceID string, eventJSON string) (string, error) {
	var object map[string]interface{}
	err := json.Unmarshal([]byte(eventJSON), &object)
	if err != nil {
		return "", err
	}
	event := make(map[string]interface{})
	event["event"] = object
	err = c.p.session.DB(c.p.dbName).C(c.p.eventCollection).Insert(event)
	if err != nil {
		return "", err
	}
	err = c.p.session.DB(c.p.dbName).C(c.p.triggerCollection).Insert(bson.M{"trigger": 1})
	return "", err
}

// ReadEvents Read JSON events started from fromId to slice of strings
func (e *ListennerT) readEvents(fromID string) (chan string, error) {
	var (
		iter   *mgo.Iter
		result interface{}
	)
	outQueue := make(chan string)
	if fromID != "" {
		objID := bson.ObjectIdHex(fromID)
		iter = e.p.session.DB(e.p.dbName).C(e.p.eventCollection).Find(bson.M{"_id": bson.M{"$gt": objID}}).Iter()
	} else {
		iter = e.p.session.DB(e.p.dbName).C(e.p.eventCollection).Find(nil).Iter()
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

func (e *ListennerT) Subscribe(fromID string) (chan string, error) {
	if e.p.session == nil {
		return nil, errors.New(`Mongo isn't connected. Please use Dial().`)
	}
	cTrigger := e.p.session.DB(e.p.dbName).C(e.p.triggerCollection)
	var lastTriggerID string
	var lastEventID string
	if fromID != "" {
		lastEventID = fromID
	}
	var result map[string]interface{}
	outChan := make(chan string)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Errorf("Close closed channel. Recover from panic")
			}
		}()
		iterLast := cTrigger.Find(nil).Sort("-$natural").Limit(2).Iter()
		for iterLast.Next(&result) {
			lastTriggerID = result["_id"].(bson.ObjectId).Hex()
		}
		iterLast.Close()
		iter := cTrigger.Find(bson.M{"_id": bson.M{"$gt": bson.ObjectIdHex(lastTriggerID)}}).Sort("$natural").Tail(5 * time.Second)
		for {
			for iter.Next(&result) {
				lastTriggerID = result["_id"].(bson.ObjectId).Hex()
				evChan, err := e.readEvents(lastEventID)
				if err != nil {
					return
				}
				for s := range evChan {
					var js map[string]interface{}
					err := json.Unmarshal([]byte(s), &js)
					if err != nil {
						return
					}
					lastEventID = js["_id"].(string)
					outChan <- s
				}
			}
			if iter.Err() != nil {
				return
			}
			if iter.Timeout() {
				continue
			}
			qNext := cTrigger.Find(bson.M{"_id": bson.M{"$gt": lastTriggerID}})

			iter = qNext.Sort("$natural").Tail(5 * time.Second)
		}
		iter.Close()
		return
	}()
	return outChan, nil
}

// Unsubscribe closes channel
func (e *ListennerT) Unsubscribe(eventChannel chan string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Errorf("Close closed channel. Recover from panic")
		}
	}()
	close(eventChannel)
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
