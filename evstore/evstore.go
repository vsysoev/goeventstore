package evstore

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	chanBufSize int = 1024
)

type (
	// Connection exports mondodb connection attributes
	Connection struct {
		session           *mgo.Session
		dbName            string
		eventCollection   string
		triggerCollection string
		committer         *CommitterT
		listenner         *ListennerT
	}
	// CommitterT exports Committer interface
	CommitterT struct {
		p *Connection
	}
	// ListennerT export Listenner interface
	ListennerT struct {
		p    *Connection
		done chan bool
	}
	// Committer interface defines method to commit new event to eventstore
	Committer interface {
		SubmitEvent(sequenceID string, eventJSON string) (string, error)
	}
	// Listenner interface defines method to listen events from the datastore
	Listenner interface {
		Subscribe(fromID string) (chan string, error)
		Unsubscribe(eventChannel chan string)
	}
)

// Dial fabric class to produce new connection to eventstore
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

// Close closes connection to event store
func (c *Connection) Close() {
	c.session.Close()
}

// Committer return committer object which implement Committer interface
func (c *Connection) Committer() Committer {
	return c.committer
}

// Listenner returns listenner object which implments Listenner interface
func (c *Connection) Listenner() Listenner {
	return c.listenner
}

// SubmitEvent submittes event to event store
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
func (e *ListennerT) readEvents(fromID string) (*mgo.Iter, error) {
	var (
		iter *mgo.Iter
	)
	if fromID != "" {
		objID := bson.ObjectIdHex(fromID)
		iter = e.p.session.DB(e.p.dbName).C(e.p.eventCollection).Find(bson.M{"_id": bson.M{"$gt": objID}}).Iter()
	} else {
		iter = e.p.session.DB(e.p.dbName).C(e.p.eventCollection).Find(nil).Iter()
	}
	return iter, nil
}

// Subscribe returns channel from event store
func (e *ListennerT) Subscribe(fromID string) (chan string, error) {
	if e.p.session == nil {
		return nil, errors.New("Mongo isn't connected. Please use Dial().")
	}
	cTrigger := e.p.session.DB(e.p.dbName).C(e.p.triggerCollection)
	var lastTriggerID string
	var lastEventID string
	if fromID != "" {
		lastEventID = fromID
	}
	var result map[string]interface{}
	outChan := make(chan string, chanBufSize)
	e.done = make(chan bool)
	go func() {
		var (
			iter     *mgo.Iter
			oneEvent map[string]interface{}
		)
		defer func() {
			log.Println("Fire close(outChan)")
			iter.Close()
			close(outChan)
		}()
		log.Println("Find last two triggered ids")
		iterLast := cTrigger.Find(nil).Sort("-$natural").Limit(2).Iter()
		log.Println("Find.Sort.Limit.Iter returned")
		for iterLast.Next(&result) {
			lastTriggerID = result["_id"].(bson.ObjectId).Hex()
		}
		log.Println("Before iterLast.Close")
		iterLast.Close()
		log.Println("LasttriggerID", lastTriggerID)
		iter = cTrigger.Find(bson.M{"_id": bson.M{"$gt": bson.ObjectIdHex(lastTriggerID)}}).Sort("$natural").Tail(10 * time.Millisecond)
	Loop:
		for {
			for iter.Next(&result) {
				lastTriggerID = result["_id"].(bson.ObjectId).Hex()
				evIter, err := e.readEvents(lastEventID)
				if err != nil {
					return
				}
				for evIter.Next(&oneEvent) {
					log.Println(oneEvent)
					lastEventID = string(oneEvent["_id"].(bson.ObjectId))
					s, err := json.Marshal(oneEvent)
					if err != nil {
						return
					}
					select {
					case <-e.done:
						break Loop
					default:
						break
					}
					outChan <- string(s)
				}
				evIter.Close()
			}
			if iter.Err() != nil {
				return
			}
			select {
			case <-e.done:
				break Loop
			default:
				break
			}
			if iter.Timeout() {
				continue
			}
			qNext := cTrigger.Find(bson.M{"_id": bson.M{"$gt": lastTriggerID}})

			iter = qNext.Sort("$natural").Tail(10 * time.Millisecond)
		}
		return
	}()
	log.Println("Returned from Subscribe")
	return outChan, nil
}

// Unsubscribe closes channel
func (e *ListennerT) Unsubscribe(eventChannel chan string) {
	select {
	case <-eventChannel:
		break
	default:
		break
	}
	log.Println("Send e.done")
	e.done <- true
}

func contains(col []string, target string) bool {
	for _, s := range col {
		if s == target {
			return true
		}
	}
	return false
}
